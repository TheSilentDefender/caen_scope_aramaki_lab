import copy
import struct
import numpy as np
import queue
import threading
import time
from caen_felib import device, lib

class DataAcquisitionManager:
    def __init__(self, dig, n_acquisitions, data_format, adc_n_bits):
        """
        Initialize the data acquisition manager with threading support.

        Parameters:
            dig (object): CAEN digitizer device instance
            n_acquisitions (int): Number of acquisitions to perform
            data_format (list): Data format specification
            adc_n_bits (int): ADC bit resolution
        """
        self.dig = dig
        self.n_acquisitions = n_acquisitions
        self.data_format = data_format
        self.adc_n_bits = adc_n_bits
        
        # Create thread-safe queues
        self.acquisition_queue = queue.Queue()
        self.save_queue = queue.Queue()
        
        # Event to signal threads to stop
        self.stop_event = threading.Event()
        
        # Prepare data structures
        self.data = self.dig.endpoint.scope.set_read_data_format(data_format)

    def adc_to_mv(self, adc, adc_range=2):
        """Convert ADC values to millivolts."""
        return (adc * adc_range / (2**self.adc_n_bits - 1)) - adc_range / 2

    def acquisition_thread(self):
        """Thread for acquiring data from the digitizer."""
        self.acquisition_thread_id = threading.get_ident()
        print(f"[THREAD] Acquisition thread started. Thread ID: {self.acquisition_thread_id}")
        try:
            self.dig.cmd.ArmAcquisition()
            self.dig.cmd.SwStartAcquisition()

            for acq_count in range(self.n_acquisitions):
                if self.stop_event.is_set():
                    break
                print(f"[THREAD] Starting acquisition {acq_count + 1}/{self.n_acquisitions}")
                # Send software trigger and read data
                self.dig.cmd.SendSwTrigger()
                self.dig.endpoint.scope.read_data(-1, self.data)

                # Create a copy of the data and put it in the queue
                acquisition_data = {
                    'trigger_num': copy.deepcopy(self.data[0].value),
                    'timestamp': copy.deepcopy(self.data[1].value),
                    'waveforms': copy.deepcopy(self.data[2].value),
                    'waveform_sizes': copy.deepcopy(self.data[3].value)
                }
                
                self.acquisition_queue.put(acquisition_data)

            # Signal end of acquisition
            self.acquisition_queue.put(None)
        except Exception as e:
            print(f"Error in acquisition thread: {e}")
            self.stop_event.set()
        finally:
            self.dig.cmd.DisarmAcquisition()

    def save_thread(self):
        """Thread for saving waveform data to files."""
        self.save_thread_id = threading.get_ident()
        print(f"[THREAD] Save thread started. Thread ID: {self.save_thread_id}")

        # Open all files once at the start
        file_handles = {}
        try:
            # Initialize file handles for each channel
            for i in range(len(self.data[2].value)):  # Assuming self.data[2] is initialized with waveform data structure
                filename = f'raw_CH{i}.bin'
                file_handles[i] = open(filename, 'ab')

            while not self.stop_event.is_set():
                try:
                    # Wait for data with a timeout
                    acquisition_data = self.acquisition_queue.get(timeout=1)

                    # Check for termination signal
                    if acquisition_data is None:
                        break
                    print(f"[THREAD] Saving data from trigger {acquisition_data['trigger_num']}")

                    # Start timing
                    start_time = time.time()

                    # Unpack acquisition data
                    trigger_num = acquisition_data['trigger_num']
                    timestamp = acquisition_data['timestamp']
                    waveform_sizes = acquisition_data['waveform_sizes']
                    waveforms = acquisition_data['waveforms']

                    # Save waveforms to each file
                    for i, waveform in enumerate(waveforms):
                        f = file_handles[i]
                        f.write(struct.pack('I', trigger_num))
                        f.write(struct.pack('Q', timestamp))
                        f.write(struct.pack('I', waveform_sizes[i]))
                        f.write(struct.pack('Q', 8))  # time resolution
                        waveform_mv = self.adc_to_mv(waveform[:waveform_sizes[i]])
                        for sample in waveform_mv:
                            f.write(struct.pack('f', sample))

                    # End timing and calculate duration
                    end_time = time.time()
                    loop_duration = end_time - start_time
                    print(f"[THREAD] Save loop took {loop_duration:.4f} seconds")

                except queue.Empty:
                    # No data available, continue waiting
                    continue
        except Exception as e:
            print(f"Error in save thread: {e}")
            self.stop_event.set()
        finally:
            # Close all file handles when finished
            for f in file_handles.values():
                f.close()

    def run(self):
        """
        Run the data acquisition and saving process using threads.
        
        Returns:
            bool: True if acquisition completed successfully, False otherwise
        """
        try:
            # Create and start threads
            acq_thread = threading.Thread(target=self.acquisition_thread)
            save_thread = threading.Thread(target=self.save_thread)

            acq_thread.start()
            save_thread.start()

            # Wait for threads to complete
            acq_thread.join()
            save_thread.join()

            return not self.stop_event.is_set()
        except Exception as e:
            print(f"Error running acquisition: {e}")
            return False

# Main execution
def main():
    # Connect to the digitizer
    dig = device.connect('dig2://caen.internal/usb/51054')
    print(f'Connected to digitizer (handle={hex(dig.handle)}, name={dig.name})')

    # Set up device parameters
    n_ch = int(dig.par.NUMCH.value)
    adc_sample_rate_msps = int(dig.par.ADC_SAMPLRATE.value)
    adc_n_bits = int(dig.par.ADC_NBIT.value)
    sampling_period_ns = int(1e3 / adc_sample_rate_msps)
    fw_type = dig.par.FWTYPE.value

    print(f'Number of channels: {n_ch}, ADC sampling rate: {adc_sample_rate_msps} Msps, '
          f'ADC resolution: {adc_n_bits} bits, sampling period: {sampling_period_ns} ns, '
          f'firmware type: {fw_type}')

    # Configure acquisition parameters
    reclen_ns = 300000  # in ns
    pretrg_ns = 16336   # in ns
    dig.par.RECORDLENGTHT.value = f'{reclen_ns}'
    dig.par.PRETRIGGERT.value = f'{pretrg_ns}'
    dig.par.AcqTriggerSource.value = 'SwTrg'

    # Set up channel offsets
    for ch in dig.ch:
        ch.par.DCOffset.value = '50'

    # Define the data format
    data_format = [
        {'name': 'TRIGGER_ID', 'type': 'U32'},
        {'name': 'TIMESTAMP', 'type': 'U64'},
        {'name': 'WAVEFORM', 'type': 'U16', 'dim': 2, 'shape': [n_ch, reclen_ns]},
        {'name': 'WAVEFORM_SIZE', 'type': 'U64', 'dim': 1, 'shape': [n_ch]}
    ]

    # Apply the data format and activate the scope endpoint
    dig.endpoint.par.ActiveEndpoint.value = 'scope'

    # Create and run the acquisition manager
    acquisition_manager = DataAcquisitionManager(
        dig, 
        n_acquisitions=50, 
        data_format=data_format, 
        adc_n_bits=adc_n_bits
    )

    # Run the acquisition
    success = acquisition_manager.run()
    print(f"Acquisition {'completed successfully' if success else 'failed'}")

if __name__ == "__main__":
    main()