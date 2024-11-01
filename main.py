import copy
import logging
import struct
import numpy as np
import queue
import threading
import time
import sys
from caen_felib import device
from config import ConfigReader

logging.basicConfig(level=logging.INFO)

class DataAcquisition:
    def __init__(self, dig, adc_n_bits: int, config_reader):
        """
        Initialize the data acquisition.

        Parameters:
            dig (object): CAEN digitizer device instance
            adc_n_bits (int): ADC bit resolution
            config_reader (ConfigReader): Instance of ConfigReader with configuration settings
        """
        self.dig = dig
        self.adc_n_bits = adc_n_bits
        self.config = config_reader
        
        # Get acquisition settings from config
        self.acq_settings = self.config.get_acquisition_settings()
        
        self.data_format = [
            {"name": "TRIGGER_ID", "type": "U32"},
            {"name": "TIMESTAMP", "type": "U64"},
            {
                "name": "WAVEFORM",
                "type": "U16",
                "dim": 2,
                "shape": [
                    int(self.dig.par.NUMCH.value),
                    self.acq_settings.record_length,
                ],
            },
            {
                "name": "WAVEFORM_SIZE",
                "type": "U64",
                "dim": 1,
                "shape": [int(self.dig.par.NUMCH.value)],
            },
        ]

        self.acquisition_queue = queue.Queue()
        self.save_queue = queue.Queue()
        self.stop_event = threading.Event()
        
        # Set up data format for digitizer
        self.data = self.dig.endpoint.scope.set_read_data_format(self.data_format)

        # ADC conversion parameters
        self.adc_scale = 2.0 / (2**self.adc_n_bits - 1)
        self.adc_offset = -1.0
        
        # Apply settings to the digitizer
        self.set_settings()

    def set_settings(self):
        """Set the digitizer settings based on the configuration file."""
        try:
            # Set acquisition parameters from configuration
            self.dig.par.RECORDLENGTHT.value = str(self.acq_settings.record_length)
            self.dig.par.PRETRIGGERT.value = str(self.acq_settings.pretrigger)
            self.dig.par.AcqTriggerSource.value = self.acq_settings.acq_trigger_source
            self.dig.par.ITLAMASK.value = self.acq_settings.trigger_mask
        
            for i in range(int(self.dig.par.NUMCH.value)):
                channel_settings = self.config.get_channel_settings(i)
                self.dig.ch[i].par.DCOffset.value = str(channel_settings.dc_offset)
                self.dig.ch[i].par.TriggerThr.value = str(channel_settings.threshold)
                self.dig.ch[i].par.chenable.value = self.check_mask(self.acq_settings.selected_channels, i)

        except Exception as e:
            raise RuntimeError(f"Error applying settings to digitizer: {e}")

    def adc_to_mv(self, adc_array: np.ndarray) -> np.ndarray:
        """
        Convert ADC values to millivolts using vectorized operations.
        
        Parameters:
            adc_array (np.ndarray): Array of ADC values
            
        Returns:
            np.ndarray: Array of voltage values in millivolts
        """
        return adc_array.astype(np.float32) * self.adc_scale + self.adc_offset

    def acquisition_thread(self):
        """Thread for acquiring data from the digitizer."""
        self.acquisition_thread_id = threading.get_ident()
        logging.debug(
            f"[THREAD] Acquisition thread started. Thread ID: {self.acquisition_thread_id}"
        )
        try:
            self.dig.cmd.ArmAcquisition()
            self.dig.cmd.SwStartAcquisition()

            acq_count = 0
            while not self.stop_event.is_set():
                logging.debug(f"[THREAD] Starting acquisition {acq_count + 1}")

                self.dig.cmd.SendSwTrigger()
                self.dig.endpoint.scope.read_data(-1, self.data)

                acquisition_data = {
                    "trigger_num": copy.deepcopy(self.data[0].value),
                    "timestamp": copy.deepcopy(self.data[1].value),
                    "waveforms": copy.deepcopy(self.data[2].value),
                    "waveform_sizes": copy.deepcopy(self.data[3].value),
                }

                self.acquisition_queue.put(acquisition_data)
                acq_count += 1

            self.acquisition_queue.put(None)
            
        except Exception as e:
            print(f"Error in acquisition thread: {e}")
            self.stop_event.set()
        finally:
            self.dig.cmd.DisarmAcquisition()

    def save_thread(self):
        """Thread for saving waveform data to files with optimized writing."""
        self.save_thread_id = threading.get_ident()
        logging.debug(f"[THREAD] Save thread started. Thread ID: {self.save_thread_id}")

        file_handles = {}
        try:
            # Initialize file handles for each channel with buffering
            for i in range(len(self.data[2].value)):
                filename = f"raw_CH{i}.bin"
                file_handles[i] = open(filename, "ab", buffering=8192)  # 8KB buffer

            while not self.stop_event.is_set():
                try:
                    acquisition_data = self.acquisition_queue.get(timeout=1)
                    if acquisition_data is None:
                        break

                    logging.debug(
                        f"[THREAD] Saving data from trigger {acquisition_data['trigger_num']}"
                    )
                    start_time = time.time()

                    # Get data from the acquisition
                    trigger_num = acquisition_data["trigger_num"]
                    timestamp = acquisition_data["timestamp"]
                    waveforms = np.array(acquisition_data["waveforms"])
                    waveform_sizes = acquisition_data["waveform_sizes"]
                    
                    # Process each channel
                    for i, waveform in enumerate(waveforms):
                        f = file_handles[i]
                        size = waveform_sizes[i]

                        # Pack header data directly
                        f.write(struct.pack("I", trigger_num))
                        f.write(struct.pack("Q", timestamp))
                        f.write(struct.pack("I", size))
                        f.write(struct.pack("Q", 8))  # time resolution

                        # Convert and write waveform data
                        waveform_mv = self.adc_to_mv(waveform[:size])
                        waveform_mv.tofile(f)

                        # Flush every 10 acquisitions
                        if trigger_num % 10 == 0:
                            f.flush()

                    end_time = time.time()
                    loop_duration = end_time - start_time
                    logging.debug(f"[THREAD] Save loop took {loop_duration:.4f} seconds")

                except queue.Empty:
                    continue
                except Exception as e:
                    print(f"Error processing acquisition: {e}")
                    raise

        except Exception as e:
            print(f"Error in save thread: {e}")
            self.stop_event.set()
        finally:
            for f in file_handles.values():
                f.flush()
                f.close()

    def check_mask(self, mask, channel):
        mask = int(mask, 16)
        result = (mask & (1 << channel)) != 0
        return str(result)
    
    def print_settings(self):
        print("Current settings:")
        print(f"Record length: {self.dig.par.RECORDLENGTHT.value}")
        print(f"Pretrigger: {self.dig.par.PRETRIGGERT.value}")
        print(f"Acquisition trigger source: {self.dig.par.AcqTriggerSource.value}")
        print(f"Trigger mask: {self.dig.par.ITLAMask.value}")
        for i in range(int(self.dig.par.NUMCH.value)):
            print(f"Channel {i}:")
            print(f"DC offset: {self.dig.ch[i].par.DCOffset.value}")
            print(f"Threshold: {self.dig.ch[i].par.TriggerThr.value}")
            print(f"Channel enabled: {self.dig.ch[i].par.chenable.value}")
       
    def run(self) -> bool:
        """
        Run the data acquisition and saving process using threads.
        
        Returns:
            bool: True if acquisition completed successfully, False otherwise
        """
        try:
            acq_thread = threading.Thread(target=self.acquisition_thread)
            save_thread = threading.Thread(target=self.save_thread)

            acq_thread.start()
            save_thread.start()

            return acq_thread, save_thread  # Return threads for the main loop

        except Exception as e:
            print(f"Error running acquisition: {e}")
            return False


def listen_for_key(acquisition_manager):
    """Listen for user input to stop acquisition."""
    while True:
        user_input = input("Press 'q' to quit or 's' to print settings: ")
        if user_input.strip().lower() == 'q':
            acquisition_manager.stop_event.set()
            print("Exiting acquisition...")
            break
        if user_input.strip().lower() == 's':
            acquisition_manager.print_settings()


def main():
    default_settings = {
        "ACQ": {
            "record_length": "300000",
            "pretrigger": "16336",
            "acq_trigger_source": "SwTrg",
            "trigger_mode": "Normal",
            "selected_channels": "0x7",
            "trigger_mask": "0x0",
        },
        "default_channel": {
            "dc_offset": "50",
            "threshold": "100",
        },
    }

    try:
        dig = device.connect("dig2://caen.internal/usb/51054")
        print(f"Connected to digitizer (handle={hex(dig.handle)}, name={dig.name})")
        dig.cmd.Reset()

        n_ch = int(dig.par.NUMCH.value)
        adc_sample_rate_msps = int(dig.par.ADC_SAMPLRATE.value)
        adc_n_bits = int(dig.par.ADC_NBIT.value)
        sampling_period_ns = int(1e3 / adc_sample_rate_msps)
        fw_type = dig.par.FWTYPE.value

        print(
            f"Number of channels: {n_ch}, ADC sampling rate: {adc_sample_rate_msps} Msps, "
            f"ADC resolution: {adc_n_bits} bits, sampling period: {sampling_period_ns} ns, "
            f"firmware type: {fw_type}"
        )

        config_reader = ConfigReader("settings.ini", default_settings, n_ch)

        dig.endpoint.par.ActiveEndpoint.value = "scope"

        acquisition_manager = DataAcquisition(
            dig, 
            adc_n_bits=adc_n_bits,
            config_reader=config_reader
        )

        # Start acquisition and save threads
        acq_thread, save_thread = acquisition_manager.run()
        
        # Start the input listener in the main thread
        listen_for_key(acquisition_manager)

        # Wait for threads to finish
        acq_thread.join()
        save_thread.join()

        print("Acquisition process ended.")

    except Exception as e:
        print(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    main()
