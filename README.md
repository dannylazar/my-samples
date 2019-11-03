# my-samples
This repository contains samples of python code I wrote during my time at 'Trekeye'. 
It also contains three research projects I did, implemented in Jupyter notebooks.

## local_controller_wireless
Wireless communication code for raspberry Pi unit. 
This code is responsible for transmitting & receiving Wi-Fi and/or BLE data.

#### receiver.py
Responsible for the instantiation of all communication classes, according to configuration parameters stated in the 'sys.argv' that is passed as a command line argument.

#### BeaconScan.py
Scans the air to detect beacon broadcasts. Once a beacon packet is detected, its relevant data i.e. addr, id, major, minor and rssi is collected, timestamped and returned as string to be propogated through the rest of the data path.

#### TMeshScan.py
Responsible to receive all BLE packets and handle according to design.
Each received packet is timestamped, analyzed and relevant signals are created and propogated through the data path.
Producer-Consumer design pattern was used to ensure that the timestamp of each BLE packet is accurate as possible, using two main threads:
* 'SocketScanThread' (Producer) - pulls BLE packets from the socket and attaches the timestamp in which the packet was received. Extracts relevant packet data according to data type. The producer writes this data into two shared memories - 'unfiltered_reception_times' & 'unfiltered_datas', by using a python Condition varialbe. 
* 'ProcessReceivedDataThread' (Consumer) - Performs filtering of duplicate BLE packets. Handles each packet according to its protocol, and creates relevant signals to be propogated through the rest of the data path.

#### bleAdvertiser.py
Perform BLE transmissions of the local control unit. Receives instruction type ('start'/'stop') and data packet to be transmitted from the system server.

#### WIFIProbeScanner.py
Monitors wifi probes in order to determine which access points are around.


## research-notebooks
1. ED Control Tower - Research based on real ADT ('Admission Discharge Transfer') dataset. Performs statistical analysis of number of patients, patients' average wait times in the ED, ward admission distribution and more.

2. Neural Signals Time Series - The dataset contains electrical measurements of 104 electrodes that were taken during a one hour-long conversation, once every ~2ms. The purpose of the research is to estimate future measurement values based on past measurements. At first, I used known linear models to perform the estimation and used their results as a reference. Then I implemented a WaveNet model, that is usually used for audio time-series estimation. It can be seen that the WaveNet model produces better results than each of the linear models. Note: the dataset I used in this project is private, and therefore was not uploaded to this repository.

3. Sleep Deprivation - Performed an analysis of a questionnaire given to patients suffering from sleep deprivation issues. I performed interogation & cleaning of the data, found correlation between different features, and tried to create a model that predicts sleep deprivation according to known symptoms.
