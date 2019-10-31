# my-samples

## local_controller_wireless
Wireless communication code for raspberry Pi unit. 
This code is responsible for transmitting & receiving Wi-Fi and/or BLE data.

##### BeaconScan.py
Scans the air to detect beacon broadcasts. Once a beacon packet is detected, its relevant data i.e. addr, id, major, minor and rssi is collected, timestamped and returned as string to be propogated through the rest of the data path

##### TMeshScan.py
Responsible to receive all BLE packets and handle according to design.
Each received packet is timestamped, analyzed and relevant signals are created and propogated through the data path.
Producer-Consumer design pattern was used to ensure that the timestamp of each BLE packet is accurate as possible, using two main threads:
* 'SocketScanThread' (Producer) - pulls BLE packets from the socket and attaches the timestamp in which the packet was received. Extracts relevant packet data according to data type. The producer writes this data into two shared memories - 'unfiltered_reception_times' & 'unfiltered_datas', by using a python Condition varialbe. 
* 'ProcessReceivedDataThread' (Consumer) - Performs filtering of duplicate BLE packets. Handles each packet according to its protocol, and creates relevant signals to be propogated through the rest of the data path 
