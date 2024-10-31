import struct
import array
import numpy as np
import matplotlib.pyplot as plt

import ROOT

plt.rcParams["figure.figsize"] = (12,10)

class Event:
    def __init__(self, event_number, timestamp, nSamples, time_resolution, waveform):
        self.EventNumber = event_number
        self.Timestamp = timestamp
        self.NSamples = nSamples
        self.TimeResolution = time_resolution
        # self.Channel = channel
        self.Waveform = waveform

class ReadRawFile:
    def __init__(self, input_raw_file):
        self.input_raw_file = input_raw_file

    def read_raw_data(self):
        with open(self.input_raw_file, 'rb') as myfile:
            while True:
                try:
                    event_number = struct.unpack("<I", myfile.read(4))[0]
                    myfile.seek(0, 1)
                    timestamp = struct.unpack("<Q", myfile.read(8))[0]
                    myfile.seek(0, 1)
                    nSamples = struct.unpack("<I", myfile.read(4))[0]
                    myfile.seek(0, 1)
                    time_resolution = struct.unpack("<Q", myfile.read(8))[0]
                    # myfile.seek(0, 1)
                    # channel = struct.unpack("<i", myfile.read(4))[0]
                    wf = []
                    for _ in range(nSamples): # this is 2ms record length. 125000 is 1ms
                        myfile.seek(0, 1)
                        wf.append(struct.unpack("<f", myfile.read(4))[0])

                    waveform = array.array('f', wf)

                    # event_object = Event(event_number, timestamp, nSamples, time_resolution, channel, waveform)
                    event_object = Event(event_number, timestamp, nSamples, time_resolution, waveform)

                    yield event_object  
                except struct.error:
                    break
def main():

    input_raw_file = 'raw_CH10.bin'

    events = ReadRawFile(input_raw_file) # build event from raw file
    counter=0
    timestamp_array = []
    for ievent in events.read_raw_data(): # loop over all the events
        print(f"EventNumber: {ievent.EventNumber}")
        print(f"Timestamp: {ievent.Timestamp}")
        print(f"Num Samples: {ievent.NSamples}")
        print(f"TimeResolution: {ievent.TimeResolution}")

        print("-------------")
        
        timestamp_array.append(ievent.Timestamp*8/10**9)
        
        
        # if counter==2:break
        # counter+=1

    myBins = np.linspace(0,120, 121)
    print(myBins)
    plt.hist(np.array(timestamp_array),myBins)
    plt.grid(True)
    plt.xlabel("Time in seconds")
    plt.ylabel("Number of events per second ==> bin width of 1 second")
    plt.title("event count vs timestamp on each event when NO PPS signal is implemented")
    # plt.title("event count vs timestamp on each event when PPS signal is implemented")


if __name__ == "__main__":
    main()