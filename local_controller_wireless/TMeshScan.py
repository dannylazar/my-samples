# BLE iBeaconScanner based on https://github.com/adamf/BLE/blob/master/ble-scanner.py
# JCS 06/07/14

# BLE scanner based on https://github.com/adamf/BLE/blob/master/ble-scanner.py
# BLE scanner, based on https://code.google.com/p/pybluez/source/browse/trunk/examples/advanced/inquiry-with-rssi.py

# https://github.com/pauloborges/bluez/blob/master/tools/hcitool.c for lescan
# https://kernel.googlesource.com/pub/scm/bluetooth/bluez/+/5.6/lib/hci.h for opcodes
# https://github.com/pauloborges/bluez/blob/master/lib/hci.c#L2782 for functions used by lescan

# performs a simple device inquiry, and returns a list of ble advertizements 
# discovered device

# NOTE: Python's struct.pack() will add padding bytes unless you make the endianness explicit. Little endian
# should be used for BLE. Always start a struct.pack() format string with "<"

import os
import sys
import time
import struct
import bluetooth._bluetooth as bluez
from datetime import datetime
import threading
from threading import Thread, Condition
import copy
import logging

# Log configuration :
FORMAT = '%(asctime)-15s %(message)s'
folder = os.path.dirname(os.path.realpath(__file__))

# define log levels
NONE = 25
LOW_VERBOSITY = 20
MEDIUM_VERBOSITY = 15
HIGH_VERBOSITY = 10
FULL_VERBOSITY = 5
ACCELEROMETER_DEBOUNCE_RATE = 5

LOW_VERBOSITY_LEVEL = LOW_VERBOSITY+1
MEDIUM_VERBOSITY_LEVEL = MEDIUM_VERBOSITY+1
HIGH_VERBOSITY_LEVEL = HIGH_VERBOSITY+1

ACTIVE_HIGH_VERBOSITY_LEVEL = NONE

ACTIVE_LOCATION_VERBOSITY_LEVEL = HIGH_VERBOSITY_LEVEL
ACTIVE_ASSET_VERBOSITY_LEVEL = HIGH_VERBOSITY_LEVEL
ACTIVE_ALERT_VERBOSITY_LEVEL = HIGH_VERBOSITY_LEVEL
ACTIVE_TAG_VERBOSITY_LEVEL = HIGH_VERBOSITY_LEVEL
ACTIVE_BATTERY_VERBOSITY_LEVEL = HIGH_VERBOSITY_LEVEL
ACTIVE_AGGREGATOR_VERBOSITY_LEVEL = HIGH_VERBOSITY_LEVEL
ACTIVE_LOCATION_BEACON_VERBOSITY_LEVEL = HIGH_VERBOSITY_LEVEL
ACTIVE_MOBILE_VERBOSITY_LEVEL = HIGH_VERBOSITY_LEVEL
DEBUG_ERROR_VERBOSITY_LEVEL = 30

global LOGGER_LEVEL
LOGGER_LEVEL = NONE

BLEEP_SENSORS = {}
BLEEP_TAGS = {}
BLEEP_LOCATION_BEACONS = {}
BLEEP_ALERT_SENSORS = {}
BLEEP_SUPPLY_BUTTON = {}
BLEEP_ACCELEROMETER_SENSORS = {}
#BLEEP_ACCELEROMETER_SENSORS["c9:8c:18:61:e0:1b"] = { "last_reception": 0 }
BLEEP_TEMPERATURE_SENSORS = {}

LE_META_EVENT = 0x3e
LE_PUBLIC_ADDRESS=0x00
LE_RANDOM_ADDRESS=0x01
LE_SET_SCAN_PARAMETERS_CP_SIZE=7
OGF_LE_CTL=0x08
OCF_LE_SET_SCAN_PARAMETERS=0x000B
OCF_LE_SET_SCAN_ENABLE=0x000C
OCF_LE_CREATE_CONN=0x000D

LE_ROLE_MASTER = 0x00
LE_ROLE_SLAVE = 0x01

# these are actually subevents of LE_META_EVENT
EVT_LE_CONN_COMPLETE=0x01
EVT_LE_ADVERTISING_REPORT=0x02
EVT_LE_CONN_UPDATE_COMPLETE=0x03
EVT_LE_READ_REMOTE_USED_FEATURES_COMPLETE=0x04

# Advertisment event types
ADV_IND=0x00
ADV_DIRECT_IND=0x01
ADV_SCAN_IND=0x02
ADV_NONCONN_IND=0x03
ADV_SCAN_RSP=0x04

MAC_ADDRESS_OFFSET = 3
MAC_ADDRESS_LENGTH = 6

ADVDATA_LENGTH_OFFSET = 9

ACTIVE_ADVDATA_FIRST_BYTE_OFFSET = 10
ACTIVE_ADVDATA_OFFSET = 10

ACTIVE_RSSI_OFFSET = 32
ACTIVE_RSSI_GRANULARITY = 3


ACTIVE_SENSOR_TYPE_OFFSET = 10

ACTIVE_SENSOR_PACKET_TYPE = {
    "BATTERY"   : 0,
    "LOCATION"  : 3,
    "ASSET"     : 4,
    "ALERT"     : 5,
    "AGGREGATOR": 6,
    "MOBILE"    : 7
}

ACTIVE_TAG_PACKET_PARAMS = {
    "OFFESTS": {
        "PACKET_INDEX"  : 19,
        "FIRST_DATA"    : 18,
    },
}

ACTIVE_SENSOR_PACKET_OFFESTS = {
    "LOCATION": {
        "PACKET_INDEX"  : 11,
        "FIRST_DATA"    : 12
    },
    "ASSET": {
        "CYCLE_INDEX"   : 10,    
        "PACKET_INDEX"  : 12,
        "FIRST_DATA"    : 10
    },
    "AGGREGATOR": {
        "PACKET_INDEX"  : 11,
        "FIRST_DATA"    : 12
    },
    "ALERT": {
        "PACKET_INDEX"  : 11,
        "FIRST_DATA"    : 12
    },    
    "BATTERY": {
        "DATA_COUNT"    : 11,
        "FIRST_DATA"    : 12
    },    
    "MOBILE": {
        "PACKET_INDEX"  : 11,
        "FIRST_DATA"    : 12
    },
    "ACCELEROMETER": {
        "PACKET_INDEX"  : 0,
        "FIRST_DATA"    : 24
    },  
    "TEMPERATURE": {
        "PACKET_INDEX"  : 0,
        "FIRST_DATA"    : 30
    },  
    "SUPPLY": {
        "LOCATION_ID"   : 11,
        "SUPPLY_ID"     : 12,
        "LED_STATUS"    : 13,
        "BATTERY_STATUS": 14
    }
}

BYTES_PER_BATTERY_ITEM = 3

ACTIVE_SENSOR_ASSET_IDS_IN_CYCLE = 27

SENSOR_INDEX_TIME_CACHE_SIZE_ACTIVE = 12
SENSOR_INDEX_DATA_CACHE_SIZE_ACTIVE = 12

SENSOR_SOURCE_INDEX_KEY_CACHE_SIZE_ACTIVE = 20
COMPARE_INDEXES_THRESHOLD_ACTIVE = 128
SENSOR_INDICES_NUM_ACTIVE = 255

MAINTENANCE_NOTIFY_TIMEOUT = 1000 * 50
ALERT_MAINTENANCE_NOTIFY_TIMEOUT = 1000 * 10

mesh_beacons_lsb = {}
mesh_beacons = {}
maintenanceDataLastTimes = {}
TAG_ID_LUT = {}

global global_kill
global_kill = False

#################################
####                         ####
#################################
# logging.basicConfig(format=FORMAT, filename=(folder + "/TMeshScan_" + str(datetime.utcnow()) + ".log"), filemode='w')
logging.basicConfig(format=FORMAT, filename=(folder + "/TMeshScan.log"), filemode='w')
logger = logging.getLogger('TMeshScan')
logger.setLevel(LOGGER_LEVEL)

logger.info("Starting... (LOGGER_LEVEL=" + str(LOGGER_LEVEL) + ")")
print "TMeshScan is Starting..."
sys.stdout.flush()

def logger_message(lvl, msg, *args):
    if logger.isEnabledFor(lvl):
        if len(args) > 0 :
            logger.debug(msg, *args)
        else:
            logger.debug(msg)


def close_ble_logger():
    global LOGGER_LEVEL
    print "Change TMeshScan Log Level to " + str(NONE) + " (previously was " + str(LOGGER_LEVEL) + ")"
    sys.stdout.flush()    
    logger_message(HIGH_VERBOSITY_LEVEL, "Close .log file - LOGGER_LEVEL changed to " + str(NONE))    
    LOGGER_LEVEL = NONE
    logger.setLevel(LOGGER_LEVEL)


def open_ble_logger():
    global LOGGER_LEVEL
    print "Change TMeshScan Log Level to " + str(FULL_VERBOSITY) + " (previously was " + str(LOGGER_LEVEL) + ")"
    sys.stdout.flush()
    LOGGER_LEVEL = FULL_VERBOSITY
    logger.setLevel(LOGGER_LEVEL)
    logger_message(HIGH_VERBOSITY_LEVEL, "Open .log file - LOGGER_LEVEL changed to " + str(FULL_VERBOSITY))    


#################################
####                         ####
#################################
def miliseconds_to_datetime(timeMili):
    timeSec = timeMili/1000.0
    return datetime.fromtimestamp(timeSec).strftime('%Y-%m-%d %H:%M:%S.%f')


#################################
####                         ####
#################################
class SignalsVerfier():
    def __init__(self):
        logger_message(HIGH_VERBOSITY_LEVEL, "SignalsVerfier - verifySignal init start")
        self.signals = {}
        logger_message(HIGH_VERBOSITY_LEVEL, "SignalsVerfier - verifySignal init end")


    def verifySignal(self, sensor, source, index, time):
        logger_message(HIGH_VERBOSITY_LEVEL, "SignalsVerfier - verifySignal start")
        if not sensor in self.signals:
            self.signals[sensor] = {}

        if not source in self.signals[sensor]:
            self.signals[sensor][source] = []

        # Verify only if at least 1 item exists
        if len(self.signals[sensor][source]) > 0:
            self.verify(sensor, source, index, time)

        if (len(self.signals[sensor][source]) > 10):
            del self.signals[sensor][source][0]

        self.signals[sensor][source].append({ "index": index, "time": time })

        logger_message(HIGH_VERBOSITY_LEVEL, "ProcessReceivedDataThread, SignalsVerfier - verifySignal finished")


    def verify(self, sensor, source, index, time):
        illegal = False
        for i in range(0,len(self.signals[sensor][source])):
            packet_diff = abs(index - self.signals[sensor][source][i]["index"])
            time_diff = abs(time-self.signals[sensor][source][i]["time"])
            if packet_diff < COMPARE_INDEXES_THRESHOLD_ACTIVE:
                ## Allow some tolerance in time difference between indexes
                if ( (self.signals[sensor][source][i]["index"] < index) and (self.signals[sensor][source][i]["time"] > time) and (time_diff > 10) ):
                    logger.error("Error 1: Illegal Time detected in Sensor " + str(sensor) + " and Source " + str(source) + ": index " + str(index) + " has time " + str(time) + " and index " + str(self.signals[sensor][source][i]["index"]) + " has time " + str(self.signals[sensor][source][i]["time"]))
                    illegal = True
                ## Allow some tolerance in time difference between indexes                    
                if ( (self.signals[sensor][source][i]["index"] > index) and (self.signals[sensor][source][i]["time"] < time) and (time_diff > 10) ):
                    logger.error("Error 2: Illegal Time detected in Sensor " + str(sensor) + " and Source " + str(source) + ": index " + str(index) + " has time " + str(time) + " and index " + str(self.signals[sensor][source][i]["index"]) + " has time " + str(self.signals[sensor][source][i]["time"]))
                    illegal = True
            else:
                ## Allow some tolerance in time difference between indexes                
                if ( (self.signals[sensor][source][i]["index"] > index) and (self.signals[sensor][source][i]["time"] > time) and (time_diff > 10) ):
                    logger.error("Error 3: Illegal Time detected in Sensor " + str(sensor) + " and Source " + str(source) + ": index " + str(index) + " has time " + str(time) + " and index " + str(self.signals[sensor][source][i]["index"]) + " has time " + str(self.signals[sensor][source][i]["time"]))
                    illegal = True
                ## Allow some tolerance in time difference between indexes                    
                if ((self.signals[sensor][source][i]["index"] < index) and (self.signals[sensor][source][i]["time"] < time) and (time_diff > 10) ):
                    logger.error("Error 4: Illegal Time detected in Sensor " + str(sensor) + " and Source " + str(source) + ": index " + str(index) + " has time " + str(time) + " and index " + str(self.signals[sensor][source][i]["index"]) + " has time " + str(self.signals[sensor][source][i]["time"]))
                    illegal = True
                                   
            if illegal:
                assert False,"Error: Illegal Time detected in Sensor " + str(sensor) + " and Source " + str(source) + ": index " + str(index) + " has time " + str(time) + " and index " + str(self.signals[sensor][source][i]["index"]) + " has time " + str(self.signals[sensor][source][i]["time"])


#################################
####                         ####
#################################
def returnstringpacket(pkt):
    myString = ""
    for c in pkt:
        myString +=  "%02x" %struct.unpack("B",c)[0]
    return myString 

#################################
####                         ####
#################################
def printpacket(pkt):
    for c in pkt:
        sys.stdout.write("%02x " % struct.unpack("B",c)[0])

#################################
####                         ####
#################################
def packed_bdaddr_to_string(bdaddr_packed):
    return ':'.join('%02x'%i for i in struct.unpack("<BBBBBB", bdaddr_packed[::-1]))

#################################
####                         ####
#################################
def hci_enable_le_scan(sock):
    hci_toggle_le_scan(sock, 0x01)

#################################
####                         ####
#################################
def hci_toggle_le_scan(sock, enable):
    cmd_pkt = struct.pack("<BB", enable, 0x00)
    bluez.hci_send_cmd(sock, OGF_LE_CTL, OCF_LE_SET_SCAN_ENABLE, cmd_pkt)

#################################
####                         ####
#################################
def hci_le_set_scan_parameters(sock):
    old_filter = sock.getsockopt( bluez.SOL_HCI, bluez.HCI_FILTER, 14)
    SCAN_RANDOM = 0x01
    OWN_TYPE = SCAN_RANDOM
    SCAN_TYPE = 0x01

#################################
####                         ####
#################################
def unix_time_millis(dt):
    epoch = datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds() * 1000.0

#################################
####                         ####
#################################
def le_handle_connection_complete(pkt):
    status, handle, role, peer_bdaddr_type = struct.unpack("<BHBB", pkt[0:5])
    device_address = packed_bdaddr_to_string(pkt[5:11])
    interval, latency, supervision_timeout, master_clock_accuracy = struct.unpack("<HHHB", pkt[11:])
    print "status: 0x%02x\nhandle: 0x%04x" % (status, handle)
    print "role: 0x%02x" % role
    print "device address: ", device_address

#################################
####                         ####
#################################
def set_mesh_beacons(site_data):
    print("\n\nTMeshScan has received site_data:")    
    sys.stdout.write(str(site_data))
    sys.stdout.flush()
    logger.info("\n\nTMeshScan has received site_data:\n%s", str(site_data))
    print("\n  --active site data received:")
    if (len(site_data) == 0):
        print("         none")
    else:
        for bleep_name in site_data:
            tmesh_data = site_data[bleep_name]
            mesh_beacons[tmesh_data["address"]] = tmesh_data
            mesh_beacons[tmesh_data["address"]]["name"] = bleep_name
            mesh_beacons[tmesh_data["address"]]["unit_id"] = tmesh_data["unit_id"]
            print ("    Active bleep_name %s - tmesh_data is: %s" % (bleep_name, str(tmesh_data)))
            logger.info("    Active bleep_name %s - tmesh_data is: %s", bleep_name, str(tmesh_data))
            if tmesh_data["type"] == "sensor":
                if tmesh_data["kind"] == "accelerometer":
                    #counting sensor
                    BLEEP_ACCELEROMETER_SENSORS[tmesh_data["address"]] = { "unit_id":tmesh_data["unit_id"], "model": tmesh_data["model"], "kind": tmesh_data["kind"], "last_reception": 0, "count": -1 }
                else:
                    #bleep sensors    
                    print ("    Unit " + bleep_name + ",\taddress " + tmesh_data["address"] + " -- Added BLEEP SENSOR object")                
                    logger.info("    TMesh " + bleep_name + ",\taddress " + tmesh_data["address"] + " -- Added BLEEP SENSOR object")
                    BLEEP_SENSORS[tmesh_data["address"]] = {"name":tmesh_data["name"], "kind": tmesh_data["kind"]}
                    if tmesh_data["kind"] == "alert":
                        print ("    TMesh " + bleep_name + ",\taddress " + tmesh_data["address"] + " -- Added BLEEP ALERT SENSOR object")
                        logger.info("    TMesh " + bleep_name + ",\taddress " + tmesh_data["address"] + " -- Added BLEEP ALERT SENSOR object")
                        BLEEP_ALERT_SENSORS[tmesh_data["address"]] = {"name":tmesh_data["name"], "kind": tmesh_data["kind"]}
            elif tmesh_data["type"] == "tag":
                print ("    Unit " + bleep_name + ",\taddress " + tmesh_data["address"] + "-- Added BLEEP TAG object")
                TAG_ID_LUT[format(int(tmesh_data["tag_id"]), 'x')] = {"bleep_name":bleep_name, "unit_id":tmesh_data["unit_id"]}
                BLEEP_TAGS[tmesh_data["address"]] = True
                logger.info("    TMesh " + bleep_name + ",\taddress " + tmesh_data["address"] + " -- Added BLEEP TAG object")
            elif tmesh_data["type"] == "location_beacon":
                print ("    Unit " + bleep_name + ",\taddress " + tmesh_data["address"] + " -- Added Location Beacon object")
                TAG_ID_LUT[format(int(tmesh_data["lb_id"]), 'x')] = {"bleep_name":bleep_name, "unit_id":tmesh_data["unit_id"]}
                BLEEP_LOCATION_BEACONS[tmesh_data["address"]] = True
                logger.info("    TMesh " + bleep_name + ",\taddress " + tmesh_data["address"] + " -- Added Location Beacon object")
        print("Active site summary:\nmesh_beacons:\n%s\n\nBLEEP_SENSORS:\n%s\n\nACCELEROMETER_SENSORS:\n%s\n\nBLEEP_TAGS:\n%s\n\BLEEP_LOCATION_BEACONS:\n%s\n\nTAG_ID_LUT:\n%s\n\n" % (str(mesh_beacons), str(BLEEP_SENSORS), str(BLEEP_ACCELEROMETER_SENSORS), str(BLEEP_TAGS), str(BLEEP_LOCATION_BEACONS), str(TAG_ID_LUT)))
        logger.info("Active site summary:\nmesh_beacons:\n%s\n\nBLEEP_SENSORS:\n%s\n\nBLEEP_TAGS:\n%s\n\BLEEP_LOCATION_BEACONS:\n%s\n\nTAG_ID_LUT:\n%s\n\n" % (str(mesh_beacons), str(BLEEP_SENSORS), str(BLEEP_TAGS), str(BLEEP_LOCATION_BEACONS), str(TAG_ID_LUT)))
    print ("\n")

#################################
####                         ####
#################################
def process_system_packet(b_address, pkt, report_pkt_offset):
    #SYSTEM PACKET - let unit know of this packet and continue to next packet
    system_command = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + 11]),16)
    system_data = None
    notify_server_on_system_command(b_address, system_command, system_data)
    sys.stdout.flush()

#################################
####                         ####
#################################
def process_trekeye_system_packet(b_address, pkt, report_pkt_offset, firmware_version):
    if (firmware_version <= 5):
        command_position = 9
    elif (firmware_version > 5):
        command_position = 10

    #SYSTEM PACKET - let unit know of this packet and continue to next packet
    system_command = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET + command_position]),16)
    system_data = None
    notify_server_on_system_command(b_address, system_command, system_data)
    sys.stdout.flush()

#################################
####                         ####
#################################
def notify_server_on_system_command(b_address, system_command, system_data):
    if (system_data != None):
        print "SYSTEM PACKET [" + b_address + "," + str(system_command) + "," + str(system_data) + "]"
    else:
        print "SYSTEM PACKET [" + b_address + "," + str(system_command) + "]" 

def check_trekeye_system_packet(pkt,report_pkt_offset):
    #check trekeye 7E7A bytes 5,6
    advdata_te1_byte = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET + 5]),16)
    if (advdata_te1_byte == 126):
        advdata_te2_byte = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET + 6]),16)
        if (advdata_te2_byte == 122):
            #read firmware version - should always be at same position
            firmware_version = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET + 11]),16)
            #check system FF byte according to firmware version (8 or 9)
            if (firmware_version <= 5):   
                advdata_system_byte = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET + 8]),16)
            elif (firmware_version > 5):
                advdata_system_byte = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET + 9]),16)

            if (advdata_system_byte == 255):
                return True
    return False

#################################
####                         ####
#################################
class ReceptionTimes:
    def __init__(self):
        self.times = [{},{}]
        self.condition = Condition()
        self.processing = 0
        self.writing = 1

    def lockTimeCondition(self):
        self.condition.acquire()

    def producerReleaseTimeCondition(self):
        self.condition.notify()
        self.condition.release()

    def consumerReleaseTimeCondition(self):
        self.condition.release()

    def consumerWaitTimeCondition(self):
        self.condition.wait(0.1)  

    def isMemoryEmpty(self):
        if self.times[self.writing]:
            return False
        else:
            return True

    def switchMemory(self):
        if (self.processing == 0):
            self.processing = 1
            self.writing = 0
        else:
            self.processing = 0
            self.writing = 1
        self.times[self.writing] = {}
        logger_message(HIGH_VERBOSITY_LEVEL, "ReceptionTimes: switchMemory function was called, new parameters are - processing=%s, writing=%s", self.processing, self.writing)

        
    def getProcessing(self):
        processing = self.times[self.processing]
        logger_message(HIGH_VERBOSITY_LEVEL, "ReceptionTimes: getProcessing function was called, processing memory is %s", self.processing)
        return processing

    def getWriting(self):
        writing = self.times[self.writing]
        logger_message(HIGH_VERBOSITY_LEVEL, "ReceptionTimes: getWriting function was called, writing memory is %s", self.writing)
        return writing


#################################
####                         ####
#################################
class UnfilteredDatas:
    def __init__(self):
        self.datas = [{},{}]
        self.condition = Condition()
        self.processing = 0
        self.writing = 1

    def lockDataCondition(self):
        self.condition.acquire()

    def producerReleaseDataCondition(self):
        self.condition.notify()
        self.condition.release()

    def consumerReleaseDataCondition(self):
        self.condition.release()

    def consumerWaitDataCondition(self):
        self.condition.wait(0.1)                

    def switchMemory(self):
        if (self.processing == 0):
            self.processing = 1
            self.writing = 0
        else:
            self.processing = 0
            self.writing = 1
        self.datas[self.writing] = {}
        logger_message(HIGH_VERBOSITY_LEVEL, "UnfilteredDatas: switchMemory function was called, new parameters are - processing=%s, writing=%s", self.processing, self.writing)


    def isMemoryEmpty(self):
        if self.datas[self.writing]:
            return False
        else:
            return True

    def getProcessing(self):
        processing = self.datas[self.processing]
        logger_message(HIGH_VERBOSITY_LEVEL, "UnfilteredDatas: getProcessing function was called, processing memory is %s",self.processing)
        return processing

    def getWriting(self):
        writing = self.datas[self.writing]
        logger_message(HIGH_VERBOSITY_LEVEL, "UnfilteredDatas: getWriting function was called, writing memory is %s", self.writing)
        return writing


#################################
####                         ####
#################################
class TMeshScanner:
    global global_kill
    def __init__(self, dev_ids, filter, location_beacons, tag_sensor, notify_count=100):
        self.observers = []
        self.notify_count = notify_count
        self.unfiltered_reception_times = {}
        self.unfiltered_datas = {}
        self.sockets_ids = {}
        self.socket_threads = []
        self.process_received_data_thread = {}
        self.tag_sensor = tag_sensor
        set_mesh_beacons(filter)
        try:
            for dev_id in dev_ids:
                ## Add new Socket for dev_id 
                self.sockets_ids[dev_id] = bluez.hci_open_dev(dev_id)
                #print "ble mesh thread started for dev " + str(dev_id)
                logger.info("Opened a Socket for dev " + str(dev_id))
        except:
            print "error accessing bluetooth device..."
            sys.exit(1)

        for dev_id in self.sockets_ids:
            ## Add new PacketReceptionTimes class for dev_id
            self.unfiltered_reception_times[dev_id] = ReceptionTimes()
            ## Add new PacketUnfilteredDatas class for dev_id
            self.unfiltered_datas[dev_id] = UnfilteredDatas()
            ## Set socket data params
            hci_le_set_scan_parameters(self.sockets_ids[dev_id])
            hci_enable_le_scan(self.sockets_ids[dev_id]) 
            ## Create and start SocketScanThread for each socket with its respective parameters)
            logger.info("Start SocketScanThread for dev_id " + str(dev_id))
            socket_thread = SocketScanThread(self, dev_id, self.sockets_ids[dev_id], self.unfiltered_reception_times[dev_id], self.unfiltered_datas[dev_id], self.tag_sensor)
            socket_thread.start()
            self.socket_threads.append(socket_thread)

        ## Create & start ProcessReceivedDataThread
        self.process_received_data_thread = ProcessReceivedDataThread(self,dev_ids,self.unfiltered_reception_times,self.unfiltered_datas)
        self.process_received_data_thread.start()

    def close_ble_logger(self):
        close_ble_logger()

    def open_ble_logger(self):
        open_ble_logger()        

    def register_observer(self, observer):
        self.observers.append(observer)

    def notify_observers_hardware_parameters(self, parameters):
        # logger_message(HIGH_VERBOSITY_LEVEL, "notify_observers_hardware_parameters was called with data %s", str(parameters))
        for observer in self.observers:
            observer.notifyHardwareParameters(parameters)
 
    def notify_observers_maintenance(self, maintenance):
        for observer in self.observers:
            observer.notifyMaintenance(maintenance)

    def notify_observers_signals(self, signals):
        for observer in self.observers:
            observer.notifySignals(signals)

    def notify_observers_alerts(self, alerts):
        logger_message(HIGH_VERBOSITY_LEVEL, "notify_observers_alerts was called (%s alerts to report)", len(alerts))
        for observer in self.observers:
            observer.notifyAlerts(alerts)

    def notify_observers_supply(self, supply):
        logger_message(HIGH_VERBOSITY_LEVEL, "notify_observers_supply was called")
        for observer in self.observers:
            observer.notifySupply(supply)            

    def kill(self):
        global global_kill
        if not global_kill:
            print "TMeshScanner - setting global_kill to True..."
            sys.stdout.flush()
            global_kill = True
            for i in range(0,len(self.socket_threads)):
                self.socket_threads[i].join()
            self.process_received_data_thread.join()


#################################
####                         ####
#################################
class SocketScanThread(Thread):
    global global_kill
    def __init__(self, TMeshScanner, dev_id, socket, unfiltered_reception_times, unfiltered_datas, tag_sensor) :
        super(SocketScanThread, self).__init__()
        self.TMeshScanner = TMeshScanner
        self.dev_id = dev_id
        self.socket = socket
        self.unfiltered_reception_times = unfiltered_reception_times
        self.unfiltered_datas = unfiltered_datas
        self.tag_sensor = tag_sensor

    def run(self):
        print "SocketScanThread Started... (dev_id=" + str(self.dev_id) + ")"
        logger.info("SocketScanThread Started (dev_id=" + str(self.dev_id) + ")")
        self.process_ble_packets()
        print "thread died"
        sys.stdout.flush()


    def update_packet_reception_time(self, b_address, packet_index, millis, sensor_rssi):
        ## Update packet reception time
        self.unfiltered_reception_times.lockTimeCondition()                    
        self.write_unfiltered_reception_times(b_address, packet_index, millis, sensor_rssi)
        self.unfiltered_reception_times.producerReleaseTimeCondition()


    def handleUnrecognizedMacPacket(self, mac_address, pkt, report_pkt_offset):
        advdata_first_byte = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET]),16)
        # logger_message(HIGH_VERBOSITY_LEVEL, "handleUnrecognizedMacPacket: Device %s & MAC %s (Most Significant Byte is %s)", self.dev_id, mac_address, hex(advdata_first_byte))
        packet_after_mac = returnstringpacket(pkt[ACTIVE_ADVDATA_FIRST_BYTE_OFFSET:])
        # tag configuration system command
        # trekeye dynamic system packets are only with length 6, after mac (this includes RSSI reported from dongle!)
        if (len(packet_after_mac) == 6):
            if (advdata_first_byte == 255):
                process_system_packet(mac_address, pkt, report_pkt_offset)
            # tag listens
            elif (advdata_first_byte >> 4 == 12):
                notify_server_on_system_command(mac_address, "02", None)

    def handleLocationBeaconPacket(self, mac_address, pkt, report_pkt_offset, millis):
        advdata_first_byte = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET]),16)
        logger_message(HIGH_VERBOSITY_LEVEL, "handleLocationBeaconPacket: Device %s & MAC %s (Most Significant Byte is %s)", self.dev_id, mac_address, hex(advdata_first_byte))
        # tag configuration system command
        if (advdata_first_byte == 2):
            if (check_trekeye_system_packet(pkt,report_pkt_offset)):
                firmware_version = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET + 11]),16)
                process_trekeye_system_packet(mac_address, pkt, report_pkt_offset,firmware_version)
        elif (advdata_first_byte == 255):
            process_system_packet(mac_address, pkt, report_pkt_offset)
        # tag broadcast
        elif (self.tag_sensor["enable"]): 
            advdata_first_byte = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_TAG_PACKET_PARAMS["OFFESTS"]["FIRST_DATA"]]),16)
            if (advdata_first_byte >> 4 == 2):
                self.handleLocationBeaconBroadcastPacket(self.tag_sensor["mac_address"], pkt, report_pkt_offset, millis)
            # tag listens
            elif (advdata_first_byte >> 4 == 12):
                notify_server_on_system_command(mac_address, "02", None)

    def handleLocationBeaconBroadcastPacket(self, mac_address, pkt, report_pkt_offset, millis):
        data_length = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ADVDATA_LENGTH_OFFSET]),16) + 1
        packet_index = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_TAG_PACKET_PARAMS["OFFESTS"]["PACKET_INDEX"]]),16)
        logger_message(ACTIVE_LOCATION_BEACON_VERBOSITY_LEVEL, "handleLocationBeaconBroadcastPacket: Device %s & MAC %s - packet_index=%s, data_length=%s - received at time %s (pkt is %s)" % (self.dev_id, mac_address, hex(packet_index), data_length, miliseconds_to_datetime(millis), returnstringpacket(pkt)))
        location_beacon_broadcast_data_string = returnstringpacket(pkt[report_pkt_offset + ACTIVE_TAG_PACKET_PARAMS["OFFESTS"]["FIRST_DATA"] : report_pkt_offset + (ACTIVE_TAG_PACKET_PARAMS["OFFESTS"]["FIRST_DATA"] + data_length)])
        logger_message(ACTIVE_LOCATION_BEACON_VERBOSITY_LEVEL, "handleLocationBeaconBroadcastPacket: Device %s & MAC %s - packet_index=%s, location_beacon_broadcast_data_string is %s" % (self.dev_id, mac_address, hex(packet_index), location_beacon_broadcast_data_string))
        self.unfiltered_datas.lockDataCondition()
        self.writeActiveUnfilteredData(mac_address, "tag", location_beacon_broadcast_data_string, packet_index, millis)
        self.unfiltered_datas.producerReleaseDataCondition()


    def handleTagPacket(self, mac_address, pkt, report_pkt_offset, millis):
        advdata_first_byte = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET]),16)
        logger_message(HIGH_VERBOSITY_LEVEL, "handleTagPacket: Device %s & MAC %s (Most Significant Byte is %s)", self.dev_id, mac_address, hex(advdata_first_byte))
        # tag configuration system command
        if (advdata_first_byte == 2):
            if (check_trekeye_system_packet(pkt,report_pkt_offset)):
                firmware_version = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET + 11]),16)
                process_trekeye_system_packet(mac_address, pkt, report_pkt_offset,firmware_version)
                hardware_model = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET + self.getHardwareModelByFirmware(firmware_version)]),16)
                self.TMeshScanner.notify_observers_hardware_parameters({"address": mac_address, "model": hardware_model, "fw_version": firmware_version})                
        elif (advdata_first_byte == 255):
            process_system_packet(mac_address, pkt, report_pkt_offset)
            firmware_version = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + 12]),16)    
            self.TMeshScanner.notify_observers_hardware_parameters({"address": mac_address, "fw_version": firmware_version})            
        # tag broadcast
        elif (self.tag_sensor["enable"]): 
            advdata_first_byte = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_TAG_PACKET_PARAMS["OFFESTS"]["FIRST_DATA"]]),16)
            if (advdata_first_byte >> 4 == 2):
                self.handleTagBroadcastPacket(self.tag_sensor["mac_address"], pkt, report_pkt_offset, millis)
	        # tag listens
            elif (advdata_first_byte >> 4 == 12):
                notify_server_on_system_command(mac_address, "02", None)

    def handleTagBroadcastPacket(self, mac_address, pkt, report_pkt_offset, millis):
        data_length = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ADVDATA_LENGTH_OFFSET]),16) + 1
        packet_index = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_TAG_PACKET_PARAMS["OFFESTS"]["PACKET_INDEX"]]),16)
        logger_message(HIGH_VERBOSITY_LEVEL, "handleTagBroadcastPacket: Device %s & MAC %s - packet_index=%s, data_length=%s - received at time %s (pkt is %s)" % (self.dev_id, mac_address, hex(packet_index), data_length, miliseconds_to_datetime(millis), returnstringpacket(pkt)))
        tag_broadcast_data_string = returnstringpacket(pkt[report_pkt_offset + ACTIVE_TAG_PACKET_PARAMS["OFFESTS"]["FIRST_DATA"] : report_pkt_offset + (ACTIVE_TAG_PACKET_PARAMS["OFFESTS"]["FIRST_DATA"] + data_length)])
        logger_message(HIGH_VERBOSITY_LEVEL, "handleTagBroadcastPacket: Device %s & MAC %s - packet_index=%s, tag_broadcast_data_string is %s" % (self.dev_id, mac_address, hex(packet_index), tag_broadcast_data_string))
        self.unfiltered_datas.lockDataCondition()
        self.writeActiveUnfilteredData(mac_address, "tag", tag_broadcast_data_string, packet_index, millis)
        self.unfiltered_datas.producerReleaseDataCondition()

    def handleSensorPacket(self, mac_address, pkt, report_pkt_offset, millis):
        advdata_first_byte = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET]),16)
        sensor_rssi = int("%02x" %struct.unpack("B",pkt[-1]),16)
        logger_message(HIGH_VERBOSITY_LEVEL, "handleSensorPacket: Device %s & MAC %s (Most Significant Byte is %s)", self.dev_id, mac_address, hex(advdata_first_byte))
        ## System Packet
        if (advdata_first_byte == 2):
            if (check_trekeye_system_packet(pkt,report_pkt_offset)):
                firmware_version = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET + 11]),16)
                process_trekeye_system_packet(mac_address, pkt, report_pkt_offset,firmware_version)
                hardware_model = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_ADVDATA_FIRST_BYTE_OFFSET + self.getHardwareModelByFirmware(firmware_version)]),16)    
                self.TMeshScanner.notify_observers_hardware_parameters({"address": mac_address, "model": hardware_model, "fw_version": firmware_version})                
                return
        elif (advdata_first_byte == 255):
            process_system_packet(mac_address, pkt, report_pkt_offset)
            return

        static_type = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_SENSOR_TYPE_OFFSET]),16) >> 4
        logger_message(HIGH_VERBOSITY_LEVEL, "handleSensorPacket: Device %s & MAC %s - static_type is %s", self.dev_id, mac_address, static_type)

        if static_type == ACTIVE_SENSOR_PACKET_TYPE["LOCATION"]:
            self.handleSensorLocationPacket(mac_address, pkt, report_pkt_offset, millis, sensor_rssi)
        elif static_type == ACTIVE_SENSOR_PACKET_TYPE["ASSET"]:
            self.handleSensorAssetPacket(mac_address, pkt, report_pkt_offset, millis, sensor_rssi)
        elif static_type == ACTIVE_SENSOR_PACKET_TYPE["AGGREGATOR"]:
            self.handleSensorAggregatorPacket(mac_address, pkt, report_pkt_offset, millis, sensor_rssi)
        elif static_type == ACTIVE_SENSOR_PACKET_TYPE["ALERT"]:
            self.handleSensorAlertPacket(mac_address, pkt, report_pkt_offset, millis, sensor_rssi)
        elif static_type == ACTIVE_SENSOR_PACKET_TYPE["BATTERY"]:
            self.handleSensorBatteryPacket(mac_address, pkt, report_pkt_offset, millis, sensor_rssi)
        elif static_type == ACTIVE_SENSOR_PACKET_TYPE["MOBILE"]:
            self.handleSensorMobilePacket(mac_address, pkt, report_pkt_offset, millis, sensor_rssi)

    def handleSensorLocationPacket(self, mac_address, pkt, report_pkt_offset, millis, sensor_rssi):
        packet_index = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_SENSOR_PACKET_OFFESTS["LOCATION"]["PACKET_INDEX"]]),16)
        data_length = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ADVDATA_LENGTH_OFFSET]),16) - 2
        logger_message(ACTIVE_LOCATION_VERBOSITY_LEVEL, "handleSensorLocationPacket: Device %s & MAC %s - packet_index=%s, data_length=%s - received at time %s" % (self.dev_id, mac_address, hex(packet_index), data_length, miliseconds_to_datetime(millis)))
        self.update_packet_reception_time(mac_address, packet_index, millis, sensor_rssi)
        if (data_length > 0):
            logger_message(ACTIVE_LOCATION_VERBOSITY_LEVEL, "handleSensorLocationPacket: Device %s & MAC %s - packet_index=%s, data_length=%s - received at time %s, pkt with data is %s" % (self.dev_id, mac_address, hex(packet_index), data_length, miliseconds_to_datetime(millis), returnstringpacket(pkt)))
            sensor_location_data_string = returnstringpacket(pkt[report_pkt_offset+ACTIVE_SENSOR_PACKET_OFFESTS["LOCATION"]["FIRST_DATA"] : report_pkt_offset+(ACTIVE_SENSOR_PACKET_OFFESTS["LOCATION"]["FIRST_DATA"]+data_length)])
            self.unfiltered_datas.lockDataCondition()
            self.writeActiveUnfilteredData(mac_address, "location", sensor_location_data_string, packet_index, millis)
            self.unfiltered_datas.producerReleaseDataCondition()

    def handleSensorAssetPacket(self, mac_address, pkt, report_pkt_offset, millis, sensor_rssi):
        packet_index = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_SENSOR_PACKET_OFFESTS["ASSET"]["PACKET_INDEX"]]),16)
        cycle_index = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_SENSOR_PACKET_OFFESTS["ASSET"]["CYCLE_INDEX"]]),16) % 16
        data_length = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ADVDATA_LENGTH_OFFSET]),16)        
        logger_message(ACTIVE_ASSET_VERBOSITY_LEVEL, "handleSensorAssetPacket: Device %s & MAC %s - packet_index=%s, cycle_index=%s, data_length=%s - received at time %s" % (self.dev_id, mac_address, packet_index, hex(cycle_index), data_length, miliseconds_to_datetime(millis)))
        self.update_packet_reception_time(mac_address, packet_index, millis, sensor_rssi)
        if (data_length > 0):
            logger_message(ACTIVE_ASSET_VERBOSITY_LEVEL, "handleSensorAssetPacket: Device %s & MAC %s - packet_index=%s, cycle_index=%s, data_length=%s - received at time %s, pkt with data is %s" % (self.dev_id, mac_address, packet_index, hex(cycle_index), data_length, miliseconds_to_datetime(millis), returnstringpacket(pkt)))
            sensor_asset_data_string = returnstringpacket(pkt[report_pkt_offset+ACTIVE_SENSOR_PACKET_OFFESTS["ASSET"]["FIRST_DATA"] : report_pkt_offset+(ACTIVE_SENSOR_PACKET_OFFESTS["ASSET"]["FIRST_DATA"]+data_length)])
            self.unfiltered_datas.lockDataCondition()
            self.writeActiveUnfilteredData(mac_address, "asset", sensor_asset_data_string, packet_index, millis)
            self.unfiltered_datas.producerReleaseDataCondition()

    def handleSensorAggregatorPacket(self, mac_address, pkt, report_pkt_offset, millis, sensor_rssi):
        packet_index = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_SENSOR_PACKET_OFFESTS["AGGREGATOR"]["PACKET_INDEX"]]),16)
        data_length = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ADVDATA_LENGTH_OFFSET]),16) - 2
        logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "handleSensorAggregatorPacket: Device %s & MAC %s - packet_index=%s, data_length=%s - received at time %s" % (self.dev_id, mac_address, hex(packet_index), data_length, miliseconds_to_datetime(millis)))
        self.update_packet_reception_time(mac_address, packet_index, millis, sensor_rssi)
        if (data_length > 0):
            logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "handleSensorAggregatorPacket: Device %s & MAC %s - packet_index=%s, data_length=%s - received at time %s, pkt with data is %s" % (self.dev_id, mac_address, hex(packet_index), data_length, miliseconds_to_datetime(millis), returnstringpacket(pkt)))
            sensor_aggregator_data_string = returnstringpacket(pkt[report_pkt_offset+ACTIVE_SENSOR_PACKET_OFFESTS["AGGREGATOR"]["FIRST_DATA"] : report_pkt_offset+(ACTIVE_SENSOR_PACKET_OFFESTS["AGGREGATOR"]["FIRST_DATA"]+data_length)])
            self.unfiltered_datas.lockDataCondition()
            self.writeActiveUnfilteredData(mac_address, "aggregator", sensor_aggregator_data_string, packet_index, millis)
            self.unfiltered_datas.producerReleaseDataCondition()

    def handleSensorBatteryPacket(self, mac_address, pkt, report_pkt_offset, millis, sensor_rssi):
        battery_status_items = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_SENSOR_PACKET_OFFESTS["BATTERY"]["DATA_COUNT"]]),16)
        logger_message(ACTIVE_BATTERY_VERBOSITY_LEVEL, "handleSensorBatteryPacket: Device %s & MAC %s - battery_status_items=%s - received at time %s (pkt is %s)" % (self.dev_id, mac_address, battery_status_items, miliseconds_to_datetime(millis), returnstringpacket(pkt)))
        if (battery_status_items > 0):
            sensor_battery_data_string = returnstringpacket(pkt[report_pkt_offset+ACTIVE_SENSOR_PACKET_OFFESTS["BATTERY"]["FIRST_DATA"] : report_pkt_offset+(ACTIVE_SENSOR_PACKET_OFFESTS["BATTERY"]["FIRST_DATA"]+battery_status_items*BYTES_PER_BATTERY_ITEM)])
            self.unfiltered_datas.lockDataCondition()
            self.writeActiveUnfilteredData(mac_address, "battery", sensor_battery_data_string, 0, millis)
            self.unfiltered_datas.producerReleaseDataCondition()

    def handleSensorMobilePacket(self, mac_address, pkt, report_pkt_offset, millis, sensor_rssi):
        packet_index = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_SENSOR_PACKET_OFFESTS["MOBILE"]["PACKET_INDEX"]]),16)
        data_length = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ADVDATA_LENGTH_OFFSET]),16) - 2
        logger_message(ACTIVE_MOBILE_VERBOSITY_LEVEL, "handleSensorMobilePacket: Device %s & MAC %s - packet_index=%s, data_length=%s - received at time %s" % (self.dev_id, mac_address, hex(packet_index), data_length, miliseconds_to_datetime(millis)))
        self.update_packet_reception_time(mac_address, packet_index, millis, sensor_rssi)
        if (data_length > 0):
            logger_message(ACTIVE_MOBILE_VERBOSITY_LEVEL, "handleSensorMobilePacket: Device %s & MAC %s - packet_index=%s, data_length=%s - received at time %s, pkt with data is %s" % (self.dev_id, mac_address, hex(packet_index), data_length, miliseconds_to_datetime(millis), returnstringpacket(pkt)))
            sensor_mobile_data_string = returnstringpacket(pkt[report_pkt_offset+ACTIVE_SENSOR_PACKET_OFFESTS["MOBILE"]["FIRST_DATA"] : report_pkt_offset+(ACTIVE_SENSOR_PACKET_OFFESTS["MOBILE"]["FIRST_DATA"]+data_length)])
            self.unfiltered_datas.lockDataCondition()
            self.writeActiveUnfilteredData(mac_address, "location", sensor_mobile_data_string, packet_index, millis)
            self.unfiltered_datas.producerReleaseDataCondition()

    def handleSensorAlertPacket(self, mac_address, pkt, report_pkt_offset, millis):
        sensor_packet_index = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ACTIVE_SENSOR_PACKET_OFFESTS["ALERT"]["PACKET_INDEX"]]),16)
        data_length = int("%02x" %struct.unpack("B",pkt[report_pkt_offset + ADVDATA_LENGTH_OFFSET]),16) - 2
        logger_message(ACTIVE_ALERT_VERBOSITY_LEVEL, "handleSensorAlertPacket: Device %s & MAC %s - sensor_packet_index=%s, data_length=%s - received at time %s" % (self.dev_id, mac_address, hex(sensor_packet_index), data_length, miliseconds_to_datetime(millis)))
        self.update_packet_reception_time(mac_address, sensor_packet_index, millis, sensor_rssi)
        if (data_length > 0):
            logger_message(ACTIVE_ALERT_VERBOSITY_LEVEL, "handleSensorAlertPacket: Device %s & MAC %s - sensor_packet_index=%s, data_length=%s - received at time %s, pkt with data is %s" % (self.dev_id, mac_address, hex(sensor_packet_index), data_length, miliseconds_to_datetime(millis), returnstringpacket(pkt)))
            advdata = pkt[report_pkt_offset+ACTIVE_SENSOR_PACKET_OFFESTS["ALERT"]["FIRST_DATA"] : report_pkt_offset+(ACTIVE_SENSOR_PACKET_OFFESTS["ALERT"]["FIRST_DATA"]+data_length)]
            sensor_alert_seperated_data = self.seperateSensorAlertData(mac_address, sensor_packet_index, advdata, pkt)
            newAlertData = self.meshedAlertData(mac_address, sensor_packet_index, sensor_alert_seperated_data, millis, pkt)
            self.TMeshScanner.notify_observers_alerts(newAlertData)

    def handleAccelerometerPacket(self, mac_address, pkt, report_pkt_offset, millis):
        if ((BLEEP_ACCELEROMETER_SENSORS[mac_address]["model"] == "iB003N") and (len(returnstringpacket(pkt)) == 66)):
            sensor_accelerometer_data_string = returnstringpacket(pkt[report_pkt_offset+ACTIVE_SENSOR_PACKET_OFFESTS["ACCELEROMETER"]["FIRST_DATA"] : report_pkt_offset+ACTIVE_SENSOR_PACKET_OFFESTS["ACCELEROMETER"]["FIRST_DATA"]+6])
        elif ((BLEEP_ACCELEROMETER_SENSORS[mac_address]["model"] == "iB003N-C") and (len(returnstringpacket(pkt)) == 70)):
            sensor_accelerometer_data_string = returnstringpacket(pkt[report_pkt_offset+ACTIVE_SENSOR_PACKET_OFFESTS["ACCELEROMETER"]["FIRST_DATA"] : report_pkt_offset+ACTIVE_SENSOR_PACKET_OFFESTS["ACCELEROMETER"]["FIRST_DATA"]+10])
        else:
            return

        print "packet:" + returnstringpacket(pkt)
        sys.stdout.flush()
        self.unfiltered_datas.lockDataCondition()
        self.writeActiveUnfilteredData(mac_address, "accelerometer", sensor_accelerometer_data_string, 0, millis)
        self.unfiltered_datas.producerReleaseDataCondition()

    def handleTemperaturePacket(self, mac_address, pkt, report_pkt_offset, millis):
        sensor_accelerometer_data_string = returnstringpacket(pkt[report_pkt_offset+ACTIVE_SENSOR_PACKET_OFFESTS["TEMPERATURE"]["FIRST_DATA"] : report_pkt_offset+ACTIVE_SENSOR_PACKET_OFFESTS["TEMPERATURE"]["FIRST_DATA"]+6])
        print "data:" + sensor_accelerometer_data_string
        sys.stdout.flush()
        #self.unfiltered_datas.lockDataCondition()
        #self.writeActiveUnfilteredData(mac_address, "temperature", sensor_accelerometer_data_string, 0, millis)
        #self.unfiltered_datas.producerReleaseDataCondition()
        #newData = activeAccelerometerData(sensor_address, x_move, y_move, z_move, time)


    def write_unfiltered_reception_times(self, sensor_address, packet_index, millis, rssi):
        logger_message(HIGH_VERBOSITY_LEVEL, "write_unfiltered_reception_times: Device %s is writing the reception time of Packet index %s from Sensor %s (reception time is %s, reception_rssi is %s)", self.dev_id, hex(packet_index), sensor_address, millis, rssi)
        writing = self.unfiltered_reception_times.getWriting()
        logger_message(HIGH_VERBOSITY_LEVEL, "write_unfiltered_reception_times: Device %s writing memory is %s", self.dev_id, self.unfiltered_reception_times.writing)
        if sensor_address not in writing:
            writing[sensor_address] = {}
        if ((packet_index in writing[sensor_address]) and (millis - writing[sensor_address][packet_index]["time"] < 30)):
            logger_message(LOW_VERBOSITY_LEVEL, "Device %s Packet index %s of Sensor %s is already in unfiltered_reception_times - Not Updating Time!", self.dev_id, hex(packet_index), sensor_address)
            return
        writing[sensor_address][packet_index] = {"time": millis, "rssi": rssi}

    def writeActiveUnfilteredData(self, bleep_address, bleep_data_type, data, packet_index, millis):
        writing = self.unfiltered_datas.getWriting()
        if bleep_address not in writing:
            writing[bleep_address] = []
        logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "writeActiveUnfilteredData: Device %s is writing the data of Packet index %s from BLEEP unit %s (type=%s)", self.dev_id, hex(packet_index), bleep_address, bleep_data_type)
        writing[bleep_address].append({"bleep_data_type": bleep_data_type, "packet_index": packet_index, "packet_time": millis, "advdata": data})

    def seperateSensorAlertData(self, mac_address, packet_index, advdata, pkt):
        global mesh_beacons
        advdata_string = returnstringpacket(advdata)
        logger_message(ACTIVE_ALERT_VERBOSITY_LEVEL, "seperateSensorAlertData: Device %s & MAC %s - packet_index=%s, advdata_string=%s ", self.dev_id, mac_address, hex(packet_index), advdata_string)
        separated_data = []        
        data_pointer = 0
        advdata_pointer = 0
        while data_pointer+13 <= len(advdata_string)-1:
            tag_mac = packed_bdaddr_to_string(advdata[advdata_pointer:advdata_pointer+6])
            if tag_mac not in mesh_beacons:
                logger_message(ACTIVE_ALERT_VERBOSITY_LEVEL, "seperateSensorAlertData: Device %s & MAC %s - tag_mac %s is unknown and therefore should be filtered", self.dev_id, mac_address, tag_mac)
                data_pointer += 14
                advdata_pointer += 7
                continue
            if (tag_mac in mesh_beacons) and (mesh_beacons[tag_mac]["type"] != 'tag'):
                logger_message(ACTIVE_ALERT_VERBOSITY_LEVEL, "seperateSensorAlertData: Device %s & MAC %s - tag_mac %s is not a TAG therefore should be filtered", self.dev_id, mac_address, tag_mac)                
                data_pointer += 14
                advdata_pointer += 7
                continue
            tag_data = int("%02x" %struct.unpack("B",advdata[advdata_pointer+6]),16)
            tag_rssi = tag_data & 0x3F
            if int(tag_rssi) == 0:
                logger_message(DEBUG_ERROR_VERBOSITY_LEVEL, "seperateSensorAlertData: Device %s & MAC %s - tag_rssi %s is an INVALID value!!\nillegal advdata is: %s\nillegal pkt is: %s", self.dev_id, mac_address, tag_rssi, advdata_string, returnstringpacket(pkt))
                data_pointer += 14
                advdata_pointer += 7
                continue            
            tag_tx_type = tag_data >> 6
            logger_message(ACTIVE_ALERT_VERBOSITY_LEVEL, "seperateSensorAlertData: Device %s & MAC %s - packet_index=%s -> tag_mac=%s, tag_data=%s", self.dev_id, mac_address, hex(packet_index), tag_mac, tag_data)
            separated_data.append({"mac": tag_mac, "tx_type": tag_tx_type, "rssi":tag_rssi})
            data_pointer += 14
            advdata_pointer += 7
        return separated_data

    def meshedAlertData(self, sensor_address, sensor_packet_index, data, millis, pkt):
        global mesh_beacons
        signal_identifier = str(sensor_packet_index)
        logger_message(ACTIVE_ALERT_VERBOSITY_LEVEL, "meshedAlertData, dev_id %s: Function started with parameters - sensor_address=%s, data=%s, millis=%s", self.dev_id, sensor_address, str(data), millis)
        sensor_name = mesh_beacons[sensor_address]["name"]
        sensor_address = mesh_beacons[sensor_address]["address"]
        active_alerts = []
        for i in range(0,len(data)):
            if int(data[i]["rssi"]) > 0:
                source_rssi = (int(data[i]["rssi"]) + ACTIVE_RSSI_OFFSET) * (-1)            
                source_tx_type = data[i]["tx_type"]
                source_name = mesh_beacons[data[i]["mac"]]["name"]
                new_alert = activeAlertData(sensor_name, signal_identifier, source_name, source_tx_type, source_rssi, millis)
                logger_message(ACTIVE_ALERT_VERBOSITY_LEVEL, "meshedAlertData, dev_id %s: created new alert - %s", self.dev_id, str(new_alert))
                active_alerts.append(new_alert)
            else:
                logger_message(DEBUG_ERROR_VERBOSITY_LEVEL, "meshedAlertData, dev_id %s, sensor_address %s, sensor_packet_index %s: rssi %s is an INVALID value!!\nillegal pkt is %s", self.dev_id, sensor_address, data[i]["rssi"], returnstringpacket(pkt))
        return active_alerts

    def process_ble_packets(self):
        logger_message(HIGH_VERBOSITY_LEVEL, "process_ble_packets: Device %s Start...", self.dev_id)                       
        sys.stdout.flush()
        global global_kill
        old_filter = self.socket.getsockopt( bluez.SOL_HCI, bluez.HCI_FILTER, 14)
        # perform a device inquiry on bluetooth device #0
        # The inquiry should last 8 * 1.28 = 10.24 seconds
        # before the inquiry is performed, bluez should flush its cache of
        # previously discovered devices
        flt = bluez.hci_filter_new()
        bluez.hci_filter_all_events(flt)
        bluez.hci_filter_set_ptype(flt, bluez.HCI_EVENT_PKT)
        self.socket.setsockopt(bluez.SOL_HCI, bluez.HCI_FILTER, flt)
        while not global_kill:
            pkt = self.socket.recv(255)
            now = datetime.utcnow()
            millis = unix_time_millis(now)
            # logger_message(HIGH_VERBOSITY_LEVEL, "process_ble_packets: Device %s recevied new packet at time %s", self.dev_id, millis)
            ptype, event, plen = struct.unpack("BBB", pkt[:3])
            if event == LE_META_EVENT:
                subevent, = struct.unpack("B", pkt[3])
                pkt = pkt[4:]
                if subevent == EVT_LE_CONN_COMPLETE:
                    le_handle_connection_complete(pkt)
                elif subevent == EVT_LE_ADVERTISING_REPORT:
                    #print "advertising report"
                    num_reports = struct.unpack("B", pkt[0])[0]
                    report_pkt_offset = 0
                    for i in range(0, num_reports):
                        b_address = packed_bdaddr_to_string(pkt[report_pkt_offset+MAC_ADDRESS_OFFSET :report_pkt_offset+(MAC_ADDRESS_OFFSET+MAC_ADDRESS_LENGTH)])
                        logger_message(HIGH_VERBOSITY_LEVEL, "process_ble_packets: Device %s recevied new packet at time %s from address %s, print packet: %s", self.dev_id, millis, b_address, returnstringpacket(pkt))
                        ## Filter packets from non mesh-beacons
                        if b_address in BLEEP_SENSORS:
                            logger_message(HIGH_VERBOSITY_LEVEL, "process_ble_packets: Device %s New BLEEP_SENSORS Packet Received - %s", self.dev_id, returnstringpacket(pkt))
                            self.handleSensorPacket(b_address, pkt, report_pkt_offset, millis)
                        if b_address in BLEEP_TAGS:
                            logger_message(HIGH_VERBOSITY_LEVEL, "process_ble_packets: Device %s New BLEEP_TAGS Packet Received - %s", self.dev_id, returnstringpacket(pkt))
                            self.handleTagPacket(b_address, pkt, report_pkt_offset, millis)
                        if b_address in BLEEP_LOCATION_BEACONS:
                            logger_message(HIGH_VERBOSITY_LEVEL, "process_ble_packets: Device %s New BLEEP_LOCATION_BEACON Packet Received - %s", self.dev_id, returnstringpacket(pkt))
                            self.handleLocationBeaconPacket(b_address, pkt, report_pkt_offset, millis)                            
                        if b_address in BLEEP_ACCELEROMETER_SENSORS:
                            now = int(time.time())
                            if (now - BLEEP_ACCELEROMETER_SENSORS[b_address]["last_reception"] > ACCELEROMETER_DEBOUNCE_RATE):
                                BLEEP_ACCELEROMETER_SENSORS[b_address]["last_reception"] = now
                                self.handleAccelerometerPacket(b_address, pkt, report_pkt_offset, millis)
                        # if b_address in BLEEP_TEMPERATURE_SENSORS:
                        #     if (len(returnstringpacket(pkt)) == 74):
                        #         now = int(time.time())
                        #         print "BLEEP_TEMPERATURE_SENSORS"
                        #         print "time: " + str(now)
                        #         print "data: " + returnstringpacket(pkt)
                        #         sys.stdout.flush()
                        #         if (now - BLEEP_TEMPERATURE_SENSORS[b_address]["last_reception"] > 3):
                        #             BLEEP_TEMPERATURE_SENSORS[b_address]["last_reception"] = now
                        #             self.handleTemperaturePacket(b_address, pkt, report_pkt_offset, millis)
        self.socket.setsockopt( bluez.SOL_HCI, bluez.HCI_FILTER, old_filter )
        return 1

    def getHardwareModelByFirmware(self,version):
        if (version <= 5):
            return 10
        elif (version > 5):
            return 12

#################################
####                         ####
#################################
class SensorTimeCache:
    def __init__(self,TMeshScanner):
        self.active_cache = {}
        self.active_data = {}
        self.active_interval = {}
        self.scanner = TMeshScanner
        self.processed = {}

    def addTimeActive(self, sensor, index, time):
        if not sensor in self.active_cache:
            self.active_cache[sensor] = []

        if not sensor in self.active_data:
            self.active_data[sensor] = {}

        if len(self.active_cache[sensor]) > 0:
            if (len(self.active_cache[sensor]) >= SENSOR_INDEX_TIME_CACHE_SIZE_ACTIVE):
                logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "addTimeActive: sensor %s, need to remove oldest item from active_cache & active_data\n Cache:\n%s\nData:\n%s", sensor, str(self.active_cache[sensor]), str(self.active_data[sensor]))
                removed = self.active_cache[sensor].pop(0)
                del self.active_data[sensor][removed["index"]]
        self.active_cache[sensor].append({"index": index, "time": time})
        self.active_data[sensor][index] = time

    def cleanOldIndicesActive(self, sensor, index):
        logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "cleanOldIndicesActive: Before clean - Sensor=%s, index=%s, cache:\n%s", sensor, index, str(self.active_cache[sensor]))
        indices_to_remove = []
        if (sensor in self.active_cache):
            oldTime = self.active_data[sensor][index]
            # find all indices with older "time"
            for i in range(0, len(self.active_cache[sensor])):
                if (self.active_cache[sensor][i]["time"] <= oldTime):
                    logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL,"cleanOldIndicesActive: need to remove data No.%s from cache memory of sensor %s (was received at time %s, before oldTime=%s)", i, sensor, self.active_cache[sensor][i]["time"], oldTime)
                    indices_to_remove.append(i)
            # remove all indices with older "time"
            logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL,"cleanOldIndicesActive: indices_to_remove array - %s", str(indices_to_remove))
            for index in indices_to_remove:
                logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL,"cleanOldIndicesActive: need to remove data No.%s from cache memory of sensor %s\nCache:\n%s\nData:\n%s", index, sensor, str(self.active_cache[sensor]), str(self.active_data[sensor]))
                removed = self.active_cache[sensor][index]
                del self.active_data[sensor][removed["index"]]
            self.active_cache[sensor] = [x for i,x in enumerate(self.active_cache[sensor]) if i not in indices_to_remove]
            logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "cleanOldIndicesActive: After clean - Sensor=%s, index=%s, cache:\n%s", sensor, index, str(self.active_cache[sensor]))
        else:
            logger.warn("cleanOldIndicesActive: Sensor %s is not in cache (check this)" % (sensor))

    def sortActiveReceptionTimes(self,sensor):
        if (len(self.active_cache[sensor]) > 1):
            self.active_cache[sensor].sort(key=lambda x: x["time"])
        logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "sortActiveReceptionTimes finished")    
    
    def sortSensorReceptionTimes(self,sensor):
        self.sortActiveReceptionTimes(sensor)

    def printSensorCache(self,sensor):
        sensor_indices_string = ', '.join(str(index) for index in self.active_cache[sensor])
        logger_message(HIGH_VERBOSITY_LEVEL, "Cached indices for Sensor %s (%s indices exist in cache): %s", sensor, len(self.active_cache[sensor]), sensor_indices_string)            

    def sensorResetActions(self, sensor):
        logger_message(HIGH_VERBOSITY_LEVEL, "Sensor %s is being reset - sensorResetActions started", sensor)
        del self.active_cache[sensor]
        del self.active_data[sensor]
        del self.active_interval[sensor]

    def getTimeActive(self,sensor,index):
        if sensor in self.active_data:
            if index in self.active_data[sensor]:
                return self.active_data[sensor][index]
            else:
                logger_message(HIGH_VERBOSITY_LEVEL, "getTimeActive: index %s reception_time does not exist in sensor %s", index, sensor)
        else:
            logger_message(HIGH_VERBOSITY_LEVEL, "getTimeActive: sensor %s does not exist in SensorTimeCache", sensor)
        return -1




#################################
####                         ####
#################################
class SensorDataCache:
    def __init__(self):
        self.active_data_cache = {}
        self.active_keys_cache = {}
        self.active_keys_array = {}

    def addActiveKey(self, sensor, key, time):
        logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "addActiveKey: Add new key %s to sensor %s", key, sensor)        
        if not sensor in self.active_keys_array:
            self.active_keys_array[sensor] = []
            self.active_keys_cache[sensor] = {}
        if (len(self.active_keys_array[sensor]) >= SENSOR_INDEX_DATA_CACHE_SIZE_ACTIVE):
            logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "addActiveKey: sensor %s, need to remove oldest item from active_keys_array & active_keys_cache:\nactive_keys_array:\n%s\nactive_keys_cache is:", sensor, str(self.active_keys_array[sensor]))
            # self.printActiveKeysCache(sensor)
            key_to_remove = self.active_keys_array[sensor].pop(0)
            del self.active_keys_cache[sensor][key_to_remove]
        self.active_keys_array[sensor].append(key)
        self.active_keys_cache[sensor][key] = time

    def addActiveData(self, sensor, sensor_type, sensor_index, sensor_time, source, sensor_data):
        if not sensor in self.active_data_cache:
            self.active_data_cache[sensor] = {}
        if not source in self.active_data_cache[sensor]:
            self.active_data_cache[sensor][source] = []
        if (len(self.active_data_cache[sensor][source]) >= SENSOR_SOURCE_INDEX_KEY_CACHE_SIZE_ACTIVE):
            self.active_data_cache[sensor][source].pop(0)
        ## add new data to 'active_data_cache' memory
        logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "addActiveData: adding data from sensor %s (sensor_type=%s, sensor_index=%s)", sensor, sensor_type, sensor_index)
        self.active_data_cache[sensor][source].append({"sensor_type": sensor_type, "sensor_index": sensor_index, "sensor_time": sensor_time, "sensor_data": sensor_data})

    def hasActiveKey(self,sensor,key,time):
        logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "hasActiveKey: check if sensor %s already contains key %s", sensor, key)             
        if sensor in self.active_keys_cache:
            if key in self.active_keys_cache[sensor]:
                if time - self.active_keys_cache[sensor][key] < 5000:
                    logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "hasActiveKey: %s already contains key %s that is VALID (time=%s, old_key_time=%s)", sensor, key, time, self.active_keys_cache[sensor][key])             
                    return True
                logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "hasActiveKey: %s already contains key %s BUT it is INVALID (time=%s, old_key_time=%s), remove key from active_keys_array", sensor, key, time, self.active_keys_cache[sensor][key])
                self.active_keys_array[sensor].remove(key)
        return False

    def sortLineDataActive(self, sensor, source):
        if len(self.active_data_cache[sensor][source]) > 1:        
            self.active_data_cache[sensor][source].sort(key=lambda x: x["sensor_time"])

    def printActiveKeysCache(self, sensor):
        activeKeysCacheString = ""
        for key in self.active_keys_cache[sensor].keys():
            activeKeysCacheString += "key %s, time %s\n" % (key, self.active_keys_cache[sensor][key])
        logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "printActiveKeysCache: Cached indices for Sensor %s (%s indices exist in line):\n%s", sensor, len(self.active_keys_cache[sensor].keys()), activeKeysCacheString)

    def createActiveSignals(self, sensor, source_id):
        allActiveData = []
        logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "createActiveSignals: Creating signals for line %s::%s - number of indices is %s", sensor, source_id, len(self.active_data_cache[sensor][source_id]))
        while self.active_data_cache[sensor][source_id]:
            currData = self.active_data_cache[sensor][source_id].pop(0)
            sensor_type = currData["sensor_type"]
            logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "createActiveSignals: Creating signals for line %s::%s - sensor_type is %s", sensor, source_id, sensor_type)
            if sensor_type == "tag":
                newData = self.createActiveTagSignals(sensor, source_id, currData)                
            elif sensor_type == "location":
                newData = self.createActiveLocationSignals(sensor, source_id, currData)
            elif sensor_type == "aggregator":
                newData = self.createActiveAggregatorSignals(sensor, source_id, currData)
            elif sensor_type == "asset":
                newData = self.createActiveAssetSignals(sensor, source_id, currData)
            elif sensor_type == "battery":
                newData = self.createActiveBatterySignals(sensor, source_id, currData)

            if not (newData is None):
                allActiveData.extend(newData)
        logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "createActiveSignals: Creating signals for line %s::%s - finished", sensor, source_id)
        return allActiveData

    def createActiveTagSignals(self, sensor, source_id, data):
        logger_message(HIGH_VERBOSITY_LEVEL, "createActiveTagSignals: Sensor %s, Source %s - print data: %s", sensor, source_id, str(data))
        source_packet_index = data["sensor_index"]
        sensor_packet_time = data["sensor_time"]
        sensor_data = data["sensor_data"]
        logger_message(HIGH_VERBOSITY_LEVEL, "createActiveTagSignals: Creating a new signal for line %s::%s - source_packet_index=%s, sensor_packet_time=%s, sensor_data=%s", sensor, source_id, source_packet_index, sensor_packet_time, str(sensor_data))
        newData = self.meshedActiveSignal(sensor, 'tag', source_packet_index, sensor_packet_time, source_id, sensor_data)
        logger_message(HIGH_VERBOSITY_LEVEL, "createActiveTagSignals: newData created\n%s", str(newData))
        if not (newData is None):
            return [newData]
        else:
            return None

    def createActiveLocationSignals(self, sensor_address, source_id, data):
        logger_message(ACTIVE_LOCATION_VERBOSITY_LEVEL, "createActiveLocationSignals: Sensor %s, Source %s - print data: %s", sensor_address, source_id, str(data))
        sensor_packet_index = data["sensor_index"]
        sensor_packet_time = data["sensor_time"]
        sensor_data = data["sensor_data"]               
        logger_message(ACTIVE_LOCATION_VERBOSITY_LEVEL, "createActiveLocationSignals: Creating a new signal for line %s::%s - sensor_packet_index=%s, sensor_packet_time=%s, sensor_data=%s", sensor_address, source_id, sensor_packet_index, sensor_packet_time, str(sensor_data))
        newData = self.meshedActiveSignal(sensor_address, 'location', sensor_packet_index, sensor_packet_time, source_id, sensor_data)
        logger_message(ACTIVE_LOCATION_VERBOSITY_LEVEL, "createActiveLocationSignals: newData Created\n%s", str(newData))                
        if not (newData is None):
            return [newData]
        else:
            return None

    def createActiveAssetSignals(self, sensor_address, source_id, data):
        logger_message(ACTIVE_ASSET_VERBOSITY_LEVEL, "createActiveAssetSignals: Sensor %s, Source %s - print data: %s", sensor_address, source_id, str(data))
        sensor_packet_index = data["sensor_index"]
        sensor_packet_time = data["sensor_time"]
        sensor_data = data["sensor_data"]
        logger_message(ACTIVE_ASSET_VERBOSITY_LEVEL, "createActiveAssetSignals: Creating a new signal for line %s::%s - sensor_packet_index=%s, sensor_packet_time=%s, sensor_data=%s", sensor_address, source_id, sensor_packet_index, sensor_packet_time, str(sensor_data))
        newData = self.meshedActiveSignal(sensor_address, 'asset', sensor_packet_index, sensor_packet_time, source_id, sensor_data)
        logger_message(ACTIVE_ASSET_VERBOSITY_LEVEL, "createActiveAssetSignals: newData Created\n%s", str(newData))
        if not (newData is None):
            return [newData]
        else:
            return None
    
    def createActiveAggregatorSignals(self, sensor_address, source_id, data):
        logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "createActiveAggregatorSignals: Sensor %s, Source %s - data: %s", sensor_address, source_id, str(data))
        sensor_packet_index = data["sensor_index"]
        sensor_packet_time = data["sensor_time"]
        sensor_data = data["sensor_data"]
        logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "createActiveAggregatorSignals: Creating a new signal for line %s::%s - sensor_packet_index=%s, sensor_packet_time=%s, sensor_data=%s", sensor_address, source_id, sensor_packet_index, sensor_packet_time, str(sensor_data))
        newData = self.meshedAggregatorSignal(sensor_address, 'aggregator', sensor_packet_index, sensor_packet_time, source_id, sensor_data)
        logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "createActiveAggregatorSignals: newData Created")
        logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "%s", str(newData))        
        if not (newData is None):
            return [newData]
        else:
            return None

    def createActiveBatterySignals(self, sensor_address, source_id, data):
        logger_message(ACTIVE_BATTERY_VERBOSITY_LEVEL, "createActiveBatterySignals: Sensor %s, Source %s - print data: %s", sensor_address, source_id, str(data))
        sensor_packet_index = data["sensor_index"]
        sensor_packet_time = data["sensor_time"]
        if source_id == '0000':
            source_name = mesh_beacons[sensor_address]["name"]
            source_unit_id = mesh_beacons[sensor_address]["unit_id"]
            logger_message(ACTIVE_BATTERY_VERBOSITY_LEVEL, "createActiveBatterySignals: sensor_address=%s - sensor reports on itself (sensor name is %s)", sensor_address, source_name)
        elif format(int(source_id,16), 'x') in TAG_ID_LUT:
            source_name = TAG_ID_LUT[format(int(source_id,16), 'x')]["bleep_name"]
            source_unit_id = TAG_ID_LUT[format(int(source_id,16), 'x')]["unit_id"]
            logger_message(ACTIVE_BATTERY_VERBOSITY_LEVEL, "createActiveBatterySignals: sensor_address=%s - sensor reports on source with ID %s (source name is %s, source_unit_id is %s)", sensor_address, source_id, source_name, source_unit_id)
        else:
            # logger_message(ACTIVE_BATTERY_VERBOSITY_LEVEL, "createActiveBatterySignals: sensor_address=%s - sensor reports on source with unrecognized ID - ignore", sensor_address, source_id)
            return None
        source_battery = data["sensor_data"]
        newData = activeBatteryData(sensor_address, source_name, source_unit_id, source_battery, sensor_packet_time)        
        logger_message(ACTIVE_BATTERY_VERBOSITY_LEVEL, "createActiveBatterySignals: newData Created\n%s", str(newData))
        return [newData]

    def meshedAggregatorSignal(self, sensor_address, sensor_type, sensor_packet_index, sensor_packet_time, source_tag_id, sensor_data): #source_tx_power, source_rssi, source_count):
        sensor_name = mesh_beacons[sensor_address]["name"]
        sensor_unit_id = mesh_beacons[sensor_address]["unit_id"]
        if (source_tag_id in TAG_ID_LUT):
            source_name = TAG_ID_LUT[source_tag_id]["bleep_name"]
            source_unit_id = TAG_ID_LUT[source_tag_id]["unit_id"]            
        else:
            return None
        signal_identifier = str(sensor_packet_index)
        logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "meshedAggregatorSignal: sensor_address=%s, sensor_type=%s, source_tag_id=%s, sensor_packet_index=%s, sensor_packet_time=%s, sensor_data=%s", sensor_address, sensor_type, source_tag_id, sensor_packet_index, sensor_packet_time, str(sensor_data))
        new_signal = activeSignalSensorAggregatedData(sensor_name, sensor_unit_id, signal_identifier, source_tag_id, source_name, source_unit_id, sensor_data, sensor_packet_time)
        return new_signal

    def meshedActiveSignal(self, sensor_address, sensor_type, sensor_packet_index, sensor_packet_time, source_tag_id, sensor_data):
        sensor_name = mesh_beacons[sensor_address]["name"]
        sensor_unit_id = mesh_beacons[sensor_address]["unit_id"]
        if (source_tag_id in TAG_ID_LUT):
            source_name = TAG_ID_LUT[source_tag_id]["bleep_name"]
            source_unit_id = TAG_ID_LUT[source_tag_id]["unit_id"]
        else:
            return None
        signal_identifier = str(sensor_packet_index)
        logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "meshedActiveSignal: sensor_address=%s, sensor_unit_id=%s, sensor_type=%s, source_tag_id=%s, sensor_packet_index=%s, sensor_packet_time=%s, sensor_data=%s", sensor_address, sensor_unit_id, sensor_type, source_tag_id, sensor_packet_index, sensor_packet_time, str(sensor_data))
        new_signal = activeSignalSensorData(sensor_name, sensor_unit_id, signal_identifier, source_tag_id, source_name, source_unit_id, sensor_data, sensor_packet_time)
        return new_signal

    def createAccelerometerSignal(self, sensor_address, bleep_data):
        data = bleep_data[0]["advdata"]
        time = bleep_data[0]["packet_time"]
        if ((BLEEP_ACCELEROMETER_SENSORS[sensor_address]["model"] == "iB003N") or (BLEEP_ACCELEROMETER_SENSORS[sensor_address]["model"] == "iB003N-C")):
            x_data = "0x"+data[0:4]
            y_data = "0x"+data[4:8]
            z_data = "0x"+data[8:12]
            x_move = int(x_data,16)
            y_move = int(y_data,16)
            z_move = int(z_data,16)

        if (BLEEP_ACCELEROMETER_SENSORS[sensor_address]["model"] == "iB003N-C"):
            count_data = "0x"+data[12:16]
            count = int(count_data,16)
            if (BLEEP_ACCELEROMETER_SENSORS[sensor_address]["count"] == count):
                return None
            else:
                BLEEP_ACCELEROMETER_SENSORS[sensor_address]["count"] = count
        else:
            count = -1
        logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "createAccelerometerSignal: Sensor %s", sensor_address)
        new_signal = activeAccelerometerData(BLEEP_ACCELEROMETER_SENSORS[sensor_address]["unit_id"], sensor_address, x_move, y_move, z_move, count, time)
        logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "createAccelerometerSignal: new_signal Created")
        logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "%s", str(new_signal))
        if not (new_signal is None):
            return [new_signal]
        else:
            return None


#################################
####                         ####
#################################
class ProcessReceivedDataThread(Thread):
    global global_kill
    def __init__(self, TMeshScanner, dev_ids, unfiltered_reception_times, unfiltered_datas) :
        super(ProcessReceivedDataThread, self).__init__()
        self.tmesh_scanner = TMeshScanner
        self.dev_ids = dev_ids
        self.unfiltered_reception_times = unfiltered_reception_times
        self.unfiltered_datas = unfiltered_datas
        self.processed_signals_data = SensorDataCache()
        self.packet_reception_times = SensorTimeCache(TMeshScanner)
        self.allSignalData = []

    def run(self):
        global global_kill
        logger.info("ProcessReceivedDataThread Started...")
        while not global_kill:
            time.sleep(0.1) 
            unfiltered_reception_times_copy, unfiltered_datas_copy = self.copy_unfiltered_data()
            logger_message(HIGH_VERBOSITY_LEVEL, "process_unfiltered_reception_times started")
            self.process_unfiltered_reception_times(unfiltered_reception_times_copy)
            logger_message(HIGH_VERBOSITY_LEVEL, "process_unfiltered_reception_times finished")
            logger_message(HIGH_VERBOSITY_LEVEL, "process_unfiltered_data started")
            allSignalData = self.process_unfiltered_data(unfiltered_datas_copy)
            logger_message(HIGH_VERBOSITY_LEVEL, "process_unfiltered_data finished")
            if len(allSignalData) > 0:
                self.tmesh_scanner.notify_observers_signals(allSignalData)

    def copy_unfiltered_data(self):
        logger_message(HIGH_VERBOSITY_LEVEL, "copy_unfiltered_data: Function started")
        unfiltered_reception_times_copy = {}    
        unfiltered_datas_copy = {}
        for dev_id in self.dev_ids:
            ## Lock unfiltered data of dev_id
            self.unfiltered_datas[dev_id].lockDataCondition()
            ## Check if there is data to process - if not then Wait()            
            if not self.unfiltered_datas[dev_id].isMemoryEmpty():
                logger_message(HIGH_VERBOSITY_LEVEL, "copy_unfiltered_data: Device %s unfiltered_datas contains new data", dev_id)
            else:
                logger_message(HIGH_VERBOSITY_LEVEL, "copy_unfiltered_data: Device %s unfiltered_datas is empty", dev_id)

            #     self.unfiltered_datas[dev_id].consumerWaitDataCondition() 
            ## Switch memories roles (writing & processing)
            logger_message(HIGH_VERBOSITY_LEVEL, "copy_unfiltered_data: Device %s unfiltered_datas switch memories", dev_id)
            self.unfiltered_datas[dev_id].switchMemory()  
            ## Release unfiltered data of dev_id
            self.unfiltered_datas[dev_id].consumerReleaseDataCondition()    
            unfiltered_datas_copy[dev_id] = self.unfiltered_datas[dev_id].getProcessing()

            ## Lock unfiltered reception times of dev_id
            self.unfiltered_reception_times[dev_id].lockTimeCondition()
            ## Check if there are reception times to process - if not then Wait()
            if not self.unfiltered_reception_times[dev_id].isMemoryEmpty():
                logger_message(HIGH_VERBOSITY_LEVEL, "copy_unfiltered_data: Device %s unfiltered_reception_times contains new data", dev_id)
            else:
                logger_message(HIGH_VERBOSITY_LEVEL, "copy_unfiltered_data: Device %s unfiltered_reception_times is empty", dev_id)
            ## Switch memories roles (writing & processing)
            logger_message(HIGH_VERBOSITY_LEVEL, "copy_unfiltered_data: Device %s unfiltered_reception_times switch memories", dev_id)
            self.unfiltered_reception_times[dev_id].switchMemory()
            ## Release unfiltered reception times of dev_id
            self.unfiltered_reception_times[dev_id].consumerReleaseTimeCondition()
            ## Get unfiltered reception times of dev_id
            unfiltered_reception_times_copy[dev_id] = self.unfiltered_reception_times[dev_id].getProcessing()

        logger_message(HIGH_VERBOSITY_LEVEL, "copy_unfiltered_data: Function finished")
        return unfiltered_reception_times_copy, unfiltered_datas_copy

    def handleActiveStaticReceptionTime(self, dev_id, sensor_address, sensor_packet_index, packet_reception_time):
        if (self.packet_reception_times.getTimeActive(sensor_address, sensor_packet_index) != -1):
            logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "handleActiveStaticReceptionTime: Device %s, Sensor %s, packet_index %s - reception time SHOULD NOT BE saved, it is already stored in memory (stored packet_reception_time=%s vs packet_reception_time=%s)", dev_id, sensor_address, sensor_packet_index, self.packet_reception_times.getTimeActive(sensor_address, sensor_packet_index), packet_reception_time)
            return
        else:
            logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "handleActiveStaticReceptionTime: Device %s, Sensor %s, packet_index %s - reception time SHOULD BE saved, packet_reception_time=%s", dev_id, sensor_address, sensor_packet_index, packet_reception_time)
            self.packet_reception_times.addTimeActive(sensor_address, sensor_packet_index, packet_reception_time)

    def handleActiveSensorData(self, dev_id, bleep_address, bleep_data):
        active_lines_list = []
        for i in range (0, len(bleep_data)):
            bleep_data_type = bleep_data[i]["bleep_data_type"]
            if bleep_data_type == "location":
                line_names = self.handleActiveSensorLocation(dev_id, bleep_address, bleep_data[i])
            elif bleep_data_type == "asset":
                line_names = self.handleActiveSensorAsset(dev_id, bleep_address, bleep_data[i])
            elif bleep_data_type == "aggregator":
                line_names = self.handleActiveSensorAggregator(dev_id, bleep_address, bleep_data[i])
            elif bleep_data_type == "battery":
                line_names = self.handleActiveSensorBattery(dev_id, bleep_address, bleep_data[i])                
            elif bleep_data_type == "tag":
                line_names = self.handleActiveTagAdv(dev_id, bleep_address, bleep_data[i])

            logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "handleActiveSensorData: Device %s, bleep_address=%s - line_names contains %s lines", dev_id, bleep_address, len(line_names))                    
            for j in range (0, len(line_names)):
                line_name = line_names[j]
                if line_name not in active_lines_list:
                    active_lines_list.append(line_name)
                logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "handleActiveSensorData: Device %s, bleep_address=%s - finished handling line %s", dev_id, bleep_address, line_name)
        return active_lines_list

    def handleActiveSensorLocation(self, dev_id, sensor_address, data):
        line_names = []
        sensor_packet_index = data["packet_index"]
        sensor_packet_key = "%s::%s" % (sensor_address, sensor_packet_index)
        sensor_packet_time = data["packet_time"]        
        if self.processed_signals_data.hasActiveKey(sensor_address, sensor_packet_key, sensor_packet_time):
            logger_message(ACTIVE_LOCATION_VERBOSITY_LEVEL, "handleActiveSensorLocation: Device %s, sensor_packet_key %s SHOULD NOT be reported!", dev_id, sensor_packet_key)
        else:
            logger_message(ACTIVE_LOCATION_VERBOSITY_LEVEL, "handleActiveSensorLocation: Device %s, sensor_packet_key %s SHOULD be reported!", dev_id, sensor_packet_key)
            self.processed_signals_data.addActiveKey(sensor_address, sensor_packet_key, sensor_packet_time)
            sensor_data = data["advdata"]
            sensor_seperated_data = self.seperateSensorLocationData(dev_id, sensor_address, sensor_packet_index, sensor_data)
            for i in range (0, len(sensor_seperated_data)):
                tag_id = sensor_seperated_data[i]["id"]
                sensor_data = sensor_seperated_data[i]["sensor_data"]
                self.processed_signals_data.addActiveData(sensor_address, "location", sensor_packet_index, sensor_packet_time, tag_id, sensor_data)
                line_name = "%s::%s" % (sensor_address, tag_id)
                logger_message(ACTIVE_LOCATION_VERBOSITY_LEVEL, "handleActiveSensorLocation: Device %s, sensor_address=%s - return line_name %s", dev_id, sensor_address, line_name)
                line_names.append(line_name)
        return line_names

    def seperateSensorLocationData(self, dev_id, sensor_address, sensor_packet_index, sensor_data):
        logger_message(ACTIVE_LOCATION_VERBOSITY_LEVEL, "seperateSensorLocationData: dev_id=%s, sensor_address=%s, sensor_packet_index=%s, sensor_data=%s ", dev_id, sensor_address, hex(sensor_packet_index), sensor_data)
        data_pointer = 0 # nibble resolution
        separated_data = []
        all_tags_status = {}
        while data_pointer+5 <= len(sensor_data)-1:
            tag_id = format(int(sensor_data[data_pointer:data_pointer+4],16), 'x')
            tag_data = int(sensor_data[data_pointer+4:data_pointer+6],16)
            tag_tx_type = tag_data >> 6
            rssi = tag_data & 0x3F
            tag_rssi = (rssi + ACTIVE_RSSI_OFFSET) * (-1)
            logger_message(ACTIVE_LOCATION_VERBOSITY_LEVEL, "seperateSensorLocationData: dev_id=%s, sensor_address=%s, sensor_packet_index=%s, tag_id=%s, tag_tx_type=%s, tag_rssi=%s", dev_id, sensor_address, sensor_packet_index, tag_id, tag_tx_type, tag_rssi)
            if tag_id not in all_tags_status:
                all_tags_status[tag_id] = []
            all_tags_status[tag_id].append({"rssi": tag_rssi, "tx_type": tag_tx_type})            
            data_pointer += 6 # 4 nibbles for tag ID, 2 nibble for tag reported RSSI data
        for tag_id in all_tags_status:
            separated_data.append({"id": tag_id, "sensor_data": all_tags_status[tag_id]})
        return separated_data

    def handleActiveSensorAsset(self, dev_id, sensor_address, data):
        line_names = []
        sensor_packet_index = data["packet_index"]
        sensor_packet_key = "%s::%s" % (sensor_address, sensor_packet_index)
        sensor_packet_time = data["packet_time"]        
        if self.processed_signals_data.hasActiveKey(sensor_address, sensor_packet_key, sensor_packet_time):
            logger_message(ACTIVE_ASSET_VERBOSITY_LEVEL, "handleActiveSensorAsset: Device %s, sensor_packet_key %s SHOULD NOT be reported!", dev_id, sensor_packet_key)
        else:
            logger_message(ACTIVE_ASSET_VERBOSITY_LEVEL, "handleActiveSensorAsset: Device %s, sensor_packet_key %s SHOULD be reported!", dev_id, sensor_packet_key)
            self.processed_signals_data.addActiveKey(sensor_address, sensor_packet_key, sensor_packet_time)
            sensor_data = data["advdata"]
            sensor_seperated_data = self.seperateSensorAssetData(dev_id, sensor_address, sensor_packet_index, sensor_data)
            for i in range (0, len(sensor_seperated_data)):
                tag_id = sensor_seperated_data[i]["id"]
                sensor_data = sensor_seperated_data[i]["sensor_data"]
                self.processed_signals_data.addActiveData(sensor_address, "asset", sensor_packet_index, sensor_packet_time, tag_id, sensor_data)
                line_name = "%s::%s" % (sensor_address, tag_id)
                logger_message(ACTIVE_ASSET_VERBOSITY_LEVEL, "handleActiveSensorAsset: Device %s, sensor_address=%s - return line_name %s", dev_id, sensor_address, line_name)
                line_names.append(line_name)
        return line_names

    def seperateSensorAssetData(self, dev_id, sensor_address, sensor_packet_index, sensor_data):
        logger_message(ACTIVE_ASSET_VERBOSITY_LEVEL, "seperateSensorAssetData: dev_id=%s, sensor_address=%s, sensor_packet_index=%s, sensor_data=%s ", dev_id, sensor_address, hex(sensor_packet_index), sensor_data)
        cycle_index = int(sensor_data[1:4],16)
        sensor_data = sensor_data[6:]
        data_pointer = 0 # nibble resolution
        separated_data = []
        all_tags_status = {}
        while data_pointer+1 <= len(sensor_data)-1:
            tag_id = format(cycle_index*ACTIVE_SENSOR_ASSET_IDS_IN_CYCLE + tag_id, 'x')
            tag_data = int(sensor_data[data_pointer:data_pointer+2],16)
            if tag_data != 0:
                tag_tx_type = tag_data >> 6
                rssi = tag_data & 0x3F
                tag_rssi = (rssi + ACTIVE_RSSI_OFFSET) * (-1)          
                logger_message(ACTIVE_ASSET_VERBOSITY_LEVEL, "seperateSensorAssetData: dev_id=%s, sensor_address=%s, cycle_index=%s, sensor_packet_index=%s, tag_id=%s, tag_tx_type=%s, tag_rssi=%s", dev_id, sensor_address, cycle_index, sensor_packet_index, tag_id, tag_tx_type, tag_rssi)
                if tag_id not in all_tags_status:
                    all_tags_status[tag_id] = []
                all_tags_status[tag_id].append({"rssi": tag_rssi, "tx_type": tag_tx_type})
            data_pointer += 2 # 2 nibbles for each asset tag reported RSSI data
            tag_id += 1
        for tag_id in all_tags_status:
            separated_data.append({"id": tag_id, "sensor_data": all_tags_status[tag_id]})            
        return separated_data 

    def handleActiveSensorAggregator(self, dev_id, sensor_address, data):
        line_names = []
        sensor_packet_index = data["packet_index"]
        sensor_packet_key = "%s::%s" % (sensor_address, sensor_packet_index)
        sensor_packet_time = data["packet_time"]
        if self.processed_signals_data.hasActiveKey(sensor_address, sensor_packet_key, sensor_packet_time):
            logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "handleActiveSensorAggregator: Device %s, sensor_packet_key %s SHOULD NOT be reported!", dev_id, sensor_packet_key)
        else:
            logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "handleActiveSensorAggregator: Device %s, sensor_packet_key %s SHOULD be reported!", dev_id, sensor_packet_key)
            self.processed_signals_data.addActiveKey(sensor_address, sensor_packet_key, sensor_packet_time)
            sensor_data = data["advdata"]
            sensor_seperated_data = self.seperateSensorAggregatorData(dev_id, sensor_address, sensor_packet_index, sensor_data)
            for i in range (0, len(sensor_seperated_data)):
                tag_id = sensor_seperated_data[i]["id"]
                sensor_data = sensor_seperated_data[i]["sensor_data"]
                self.processed_signals_data.addActiveData(sensor_address, "aggregator", sensor_packet_index, sensor_packet_time, tag_id, sensor_data)
                line_name = "%s::%s" % (sensor_address, tag_id)
                logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "handleActiveSensorAggregator: Device %s, sensor_address=%s - return line_name %s", dev_id, sensor_address, line_name)
                line_names.append(line_name)
        return line_names

    def seperateSensorAggregatorData(self, dev_id, sensor_address, sensor_packet_index, sensor_data):
        logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "seperateSensorAggregatorData: dev_id=%s, sensor_address=%s, sensor_packet_index=%s, sensor_data=%s ", dev_id, sensor_address, hex(sensor_packet_index), sensor_data)
        data_pointer = 0 # nibble resolution
        separated_data = []
        all_tags_status = {}
        while data_pointer+7 <= len(sensor_data)-1:
            tag_id = format(int(sensor_data[data_pointer:data_pointer+4],16), 'x')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
            tag_data = int(sensor_data[data_pointer+4:data_pointer+6],16)
            tag_tx_type = tag_data >> 6
            rssi = tag_data & 0x3F
            tag_rssi = (rssi + ACTIVE_RSSI_OFFSET) * (-1)
            tag_count = int(sensor_data[data_pointer+6:data_pointer+8],16)
            logger_message(ACTIVE_AGGREGATOR_VERBOSITY_LEVEL, "seperateSensorAggregatorData: dev_id=%s, sensor_address=%s, sensor_packet_index=%s, tag_id=%s, tag_tx_type=%s, tag_rssi=%s, tag_count=%s", dev_id, sensor_address, sensor_packet_index, tag_id, tag_tx_type, tag_rssi, tag_count)
            if tag_id not in all_tags_status:
                all_tags_status[tag_id] = []
            all_tags_status[tag_id].append({"count": tag_count, "rssi": tag_rssi, "tx_type": tag_tx_type})
            data_pointer += 8 # 4 nibbles for tag ID, 2 nibble for tag reported avg RSSI data, 2 nibbles for count
        for tag_id in all_tags_status:
            separated_data.append({"id": tag_id, "sensor_data": all_tags_status[tag_id]})
        return separated_data    

    def handleActiveSensorBattery(self, dev_id, sensor_address, data):
        line_names = []
        sensor_packet_index = data["packet_index"]
        sensor_packet_time = data["packet_time"]
        sensor_packet_key = "%s::%s" % (sensor_address, sensor_packet_index)
        if self.processed_signals_data.hasActiveKey(sensor_address, sensor_packet_key, sensor_packet_time):
            logger_message(ACTIVE_BATTERY_VERBOSITY_LEVEL, "handleActiveSensorBattery: Device %s, sensor_packet_key %s SHOULD NOT be reported!", dev_id, sensor_packet_key)
        else:        
            logger_message(ACTIVE_BATTERY_VERBOSITY_LEVEL, "handleActiveSensorBattery: Device %s, sensor_packet_key %s SHOULD be reported!", dev_id, sensor_packet_key)
            self.processed_signals_data.addActiveKey(sensor_address, sensor_packet_key, sensor_packet_time)
            sensor_data = data["advdata"]
            sensor_seperated_data = self.seperateSensorBatteryData(dev_id, sensor_address, sensor_packet_index, sensor_data)
            for i in range (0, len(sensor_seperated_data)):
                tag_id = sensor_seperated_data[i]["id"]
                sensor_data = sensor_seperated_data[i]["sensor_data"]
                self.processed_signals_data.addActiveData(sensor_address, "battery", sensor_packet_index, sensor_packet_time, tag_id, sensor_data)
                line_name = "%s::%s" % (sensor_address, tag_id)
                logger_message(ACTIVE_BATTERY_VERBOSITY_LEVEL, "handleActiveSensorBattery: Device %s, sensor_address=%s - return line_name %s", dev_id, sensor_address, line_name)
                line_names.append(line_name)
        return line_names

    def seperateSensorBatteryData(self, dev_id, sensor_address, sensor_packet_index, sensor_data):
        global mesh_beacons
        separated_data = []
        all_tags_status = {}        
        data_pointer = 0
        while data_pointer+5 <= len(sensor_data)-1:
            tag_id = sensor_data[data_pointer:data_pointer+4]
            battery_level = int(sensor_data[data_pointer+4:data_pointer+6],16)
            logger_message(ACTIVE_BATTERY_VERBOSITY_LEVEL, "seperateSensorBatteryData: dev_id=%s, sensor_address=%s, sensor_packet_index=%s, tag_id=%s, battery_level=%s", dev_id, sensor_address, sensor_packet_index, tag_id, battery_level)
            separated_data.append({"id": tag_id, "sensor_data": battery_level})
            data_pointer += 6
        return separated_data        

    def handleActiveTagAdv(self, dev_id, sensor_address, data):
        line_names = []
        tag_packet_index = data["packet_index"]
        tag_packet_time = data["packet_time"]
        sensor_data = data["advdata"]
        sensor_seperated_data = self.seperateSensorTagData(dev_id, sensor_address, tag_packet_index, sensor_data)
        tag_id = sensor_seperated_data["id"]
        sensor_data = sensor_seperated_data["sensor_data"]
        tag_packet_key = "%s::%s::%s" % (sensor_address, tag_id, tag_packet_index)
        if self.processed_signals_data.hasActiveKey(sensor_address, tag_packet_key, tag_packet_time):
            logger_message(ACTIVE_TAG_VERBOSITY_LEVEL, "handleActiveTagAdv: Device %s, tag_packet_key %s SHOULD NOT be reported!", dev_id, tag_packet_key)
        else:        
            logger_message(ACTIVE_TAG_VERBOSITY_LEVEL, "handleActiveTagAdv: Device %s, tag_packet_key %s SHOULD be reported!", dev_id, tag_packet_key)
            self.processed_signals_data.addActiveKey(sensor_address, tag_packet_key, tag_packet_time)
            self.processed_signals_data.addActiveData(sensor_address, "tag", tag_packet_index, tag_packet_time, tag_id, sensor_data)
            line_name = "%s::%s" % (sensor_address, tag_id)
            logger_message(ACTIVE_TAG_VERBOSITY_LEVEL, "handleActiveTagAdv: Device %s, sensor_address=%s - return line_name %s", dev_id, sensor_address, line_name)
            line_names.append(line_name)
        return line_names

    def seperateSensorTagData(self, dev_id, sensor_address, tag_packet_index, sensor_data):
        logger_message(ACTIVE_TAG_VERBOSITY_LEVEL, "seperateSensorTagData: dev_id=%s, sensor_address=%s, tag_packet_index=%s, tag_data=%s ", dev_id, sensor_address, hex(tag_packet_index), sensor_data)
        tag_id = format(int(sensor_data[4:8],16), 'x')
        tag_rssi = -256 + int(sensor_data[-2:],16)
        tag_tx_type = int(sensor_data[0],16) & 0x3
        sensor_data = {"rssi": tag_rssi, "tx_type": tag_tx_type}
        logger_message(ACTIVE_TAG_VERBOSITY_LEVEL, "seperateSensorTagData: dev_id=%s, sensor_address=%s, tag_packet_index=%s, tag_id=%s, tag_tx_type=%s, tag_rssi=%s", dev_id, sensor_address, tag_packet_index, tag_id, tag_tx_type, tag_rssi)
        separated_data = {"id": tag_id, "sensor_data": sensor_data}
        return separated_data

    def process_unfiltered_reception_times(self, unfiltered_reception_times_copy):
        global maintenanceDataLastTimes
        time_millis = unix_time_millis(datetime.utcnow())
        if unfiltered_reception_times_copy:
            for dev_id in unfiltered_reception_times_copy:
                for sensor in unfiltered_reception_times_copy[dev_id]:
                    if not sensor in maintenanceDataLastTimes:
                        logger_message(HIGH_VERBOSITY_LEVEL, "process_unfiltered_reception_times: add new sensor %s to maintenanceDataLastTimes...", sensor)
                        maintenanceDataLastTimes[sensor] = 0
                    if ((BLEEP_SENSORS[sensor]["kind"] == "alert") and (time_millis - maintenanceDataLastTimes[sensor] > ALERT_MAINTENANCE_NOTIFY_TIMEOUT)):
                        max_reception_rssi = 0
                        for packet_index in unfiltered_reception_times_copy[dev_id][sensor]:
                            if unfiltered_reception_times_copy[dev_id][sensor][packet_index]["rssi"] > max_reception_rssi:
                                max_reception_rssi = unfiltered_reception_times_copy[dev_id][sensor][packet_index]["rssi"]
                        rssi = -256 + max_reception_rssi
                        logger_message(HIGH_VERBOSITY_LEVEL, "process_unfiltered_reception_times: sensor %s reception rssi is %s", sensor, str(rssi))                                        
                        new_maintenance = { "source": mesh_beacons[sensor]["name"], "address" : sensor, "time": time_millis, "interval": 0, "rssi": rssi }
                        logger_message(HIGH_VERBOSITY_LEVEL, "process_unfiltered_reception_times: ALERT sensor %s new_maintenance is %s", sensor, str(new_maintenance))                
                        self.tmesh_scanner.notify_observers_maintenance(new_maintenance)
                        maintenanceDataLastTimes[sensor] = time_millis
                    if (BLEEP_SENSORS[sensor]["kind"] == "aggregator"):
                        max_reception_rssi = 0
                        for packet_index in unfiltered_reception_times_copy[dev_id][sensor]:
                            if unfiltered_reception_times_copy[dev_id][sensor][packet_index]["rssi"] > max_reception_rssi:
                                max_reception_rssi = unfiltered_reception_times_copy[dev_id][sensor][packet_index]["rssi"]
                        rssi = -256 + max_reception_rssi
                        logger_message(HIGH_VERBOSITY_LEVEL, "process_unfiltered_reception_times: sensor %s reception rssi is %s", sensor, str(rssi))                                        
                        new_maintenance = { "source": mesh_beacons[sensor]["name"], "address" : sensor, "time": time_millis, "interval": 0, "rssi": rssi }
                        logger_message(HIGH_VERBOSITY_LEVEL, "process_unfiltered_reception_times: AGGREGATE sensor %s new_maintenance is %s", sensor, str(new_maintenance))                
                        self.tmesh_scanner.notify_observers_maintenance(new_maintenance)
                        maintenanceDataLastTimes[sensor] = time_millis
                    elif (time_millis - maintenanceDataLastTimes[sensor] > MAINTENANCE_NOTIFY_TIMEOUT):
                        max_reception_rssi = 0
                        for packet_index in unfiltered_reception_times_copy[dev_id][sensor]:
                            if unfiltered_reception_times_copy[dev_id][sensor][packet_index]["rssi"] > max_reception_rssi:
                                max_reception_rssi = unfiltered_reception_times_copy[dev_id][sensor][packet_index]["rssi"]
                        rssi = -256 + max_reception_rssi
                        logger_message(HIGH_VERBOSITY_LEVEL, "process_unfiltered_reception_times: sensor %s reception rssi is %s", sensor, str(rssi))                                        
                        new_maintenance = { "source": mesh_beacons[sensor]["name"], "address" : sensor, "time": time_millis, "interval": 0, "rssi": rssi }
                        logger_message(HIGH_VERBOSITY_LEVEL, "process_unfiltered_reception_times: REAL-TIME sensor %s new_maintenance is %s", sensor, str(new_maintenance))                
                        self.tmesh_scanner.notify_observers_maintenance(new_maintenance)
                        maintenanceDataLastTimes[sensor] = time_millis


    def process_unfiltered_data(self, unfiltered_datas_copy):
        allSignalData = []
        active_lines_list = {}
        if unfiltered_datas_copy:
            for dev_id in unfiltered_datas_copy:
                dev_active_lines = []
                for bleep_address in unfiltered_datas_copy[dev_id]:
                    bleep_data_length = len(unfiltered_datas_copy[dev_id][bleep_address])
                    logger_message(HIGH_VERBOSITY_LEVEL, "process_unfiltered_data: Device %s, bleep_address=%s, bleep_data_length=%s", dev_id, bleep_address, bleep_data_length)
                    if (bleep_address in BLEEP_SENSORS):
                        dev_active_lines.extend(self.handleActiveSensorData(dev_id, bleep_address, unfiltered_datas_copy[dev_id][bleep_address]))
                        for line_name in dev_active_lines:
                            if line_name not in active_lines_list:
                                words = line_name.split("::")
                                active_lines_list[line_name] = {"sensor": words[0], "source": words[1]}
                    elif (bleep_address in BLEEP_ACCELEROMETER_SENSORS):
                        active_signals = self.processed_signals_data.createAccelerometerSignal(bleep_address, unfiltered_datas_copy[dev_id][bleep_address])
                        if (active_signals != None):
                            allSignalData.extend(active_signals)
            logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "process_unfiltered_data: there are %s lines in active_lines_list", len(active_lines_list))                            
            for line_name in active_lines_list:
                logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "process_unfiltered_data: create signals for line %s:", line_name)                            
                line_sensor = active_lines_list[line_name]["sensor"]
                line_source = active_lines_list[line_name]["source"]
                self.processed_signals_data.sortLineDataActive(line_sensor, line_source)
                ## debug print - start ##
                logger_message(ACTIVE_HIGH_VERBOSITY_LEVEL, "process_unfiltered_data: Print Active Line %s cached keys after sorting:", line_name)
                self.processed_signals_data.printActiveKeysCache(line_sensor)
                ## debug print - end ##
                active_signals = self.processed_signals_data.createActiveSignals(line_sensor, line_source)
                allSignalData.extend(active_signals)
        return allSignalData


#################################
####                         ####
#################################
class activeSignalSensorData():
    def __init__(self, sensor_name, sensor_unit_id, signal_identifier, source_tag_id, source_name, source_unit_id, sensor_data, timemillis):
        # recognition packet index is passed to differ from same signal reports from multiple pis
        self.type = 'signal'
        self.sensor_name = sensor_name
        self.sensor_unit_id = sensor_unit_id        
        self.signal_identifier = signal_identifier
        self.source_tag_id = source_tag_id
        self.source_name = source_name
        self.source_unit_id = source_unit_id        
        self.sensor_data = sensor_data
        self.timemillis = timemillis

    def __str__(self):
        return "type: " + self.type + ", sensor_name: " + self.sensor_name + ", sensor_unit_id: " + str(self.sensor_unit_id) + ", signal_identifier: " + str(self.signal_identifier) + ", source_tag_id: " + self.source_tag_id + ", source_name: " + self.source_name + ", source_unit_id: " + str(self.source_unit_id) + ", sensor_data: " + str(self.sensor_data) + " , timemillis: " + str(self.timemillis)

#################################
####                         ####
#################################
class activeAccelerometerData():
    def __init__(self, sensor_unit_id, sensor_address, x_move, y_move, z_move, count, timemillis):
        self.type = 'accelerometer'
        self.sensor_unit_id = sensor_unit_id
        self.sensor_address = sensor_address
        self.x_move = x_move        
        self.y_move = y_move
        self.z_move = z_move
        self.count = count
        self.timemillis = timemillis

    def __str__(self):
        return "type: " + self.type + ", sensor_address: " + self.sensor_address + ", x_move: " + str(self.x_move) + ", y_move: " + str(self.y_move) + ", z_move: " + str(self.z_move) + ", count: " + str(self.count) + " , timemillis: " + str(self.timemillis)


#################################
####                         ####
#################################
class activeSignalSensorAggregatedData():
    def __init__(self, sensor_name, sensor_unit_id, signal_identifier, source_tag_id, source_name, source_unit_id, sensor_data, timemillis):
        # recognition packet index is passed to differ from same signal reports from multiple pis
        self.type = 'aggregated_signal'
        self.sensor_name = sensor_name
        self.sensor_unit_id = sensor_unit_id
        self.signal_identifier = signal_identifier
        self.source_tag_id = source_tag_id
        self.source_name = source_name
        self.source_unit_id = source_unit_id        
        self.sensor_data = sensor_data
        self.timemillis = timemillis

    def __str__(self):
        return "type: " + self.type + ", sensor_name: " + self.sensor_name + ", sensor_unit_id: " + str(self.sensor_unit_id) + ", signal_identifier: " + str(self.signal_identifier) + ", source_tag_id: " + self.source_tag_id + ", source_name: " + self.source_name + ", source_unit_id: " + str(self.source_unit_id) + ", sensor_data: " + str(self.sensor_data) + " , timemillis: " + str(self.timemillis)


#################################
####                         ####
#################################
class activeSignalTagData():
    def __init__(self, receiver_name, signal_identifier, source_id, source_name, source_tx_power, source_rssi, timemillis):
        # recognition packet index is passed to differ from same signal reports from multiple pis
        self.type = 'signal'
        self.sensor_name = receiver_name
        self.signal_identifier = signal_identifier
        self.source_id = source_id
        self.source_name = source_name
        self.tx_power = source_tx_power
        self.rssi = source_rssi
        self.timemillis = timemillis

    def __str__(self):
        return "type: " + self.type + ", sensor_name: " + self.sensor_name + ", signal_identifier: " + str(self.signal_identifier) + ", source_id: " + self.source_id + ", source_name: " + self.source_name + ", tx_power: " + str(self.tx_power) + ", rssi: " + str(self.rssi) + " , timemillis: " + str(self.timemillis)


#################################
####                         ####
#################################
class activeBatteryData():
    def __init__(self, sensor_address, source_name, source_unit_id, battery, timemillis):
        self.type = 'battery'
        self.sensor_address = sensor_address        
        self.source_name = source_name
        self.source_unit_id = source_unit_id
        self.battery = battery
        self.timemillis = timemillis

    def __str__(self):
        return "type: " + self.type + ", sensor_address: " + self.sensor_address + ", source_name: " + self.source_name + ", source_unit_id: " + str(self.source_unit_id) + ", battery: " + str(self.battery) + ", timemillis: " + str(self.timemillis)

#################################
####                         ####
#################################
class activeAlertData():
    def __init__(self, sensor_name, signal_identifier, source_name, source_tx_power, source_rssi, timemillis):
        self.sensor_name = sensor_name
        self.signal_identifier = signal_identifier
        self.source_name = source_name
        self.tx_power = source_tx_power        
        self.rssi = source_rssi
        self.timemillis = timemillis

    def __str__(self):
        return "sensor_name: " + self.sensor_name + ", source_name: " + self.source_name + ", tx_power: " + str(self.tx_power) + ", rssi: " + str(self.rssi) + ", timemillis: " + str(self.timemillis) + ", signal_identifier: " + str(self.signal_identifier)

#################################
####                         ####
#################################
class activeSupplyData():
    def __init__(self, operation, location_id, supply_id, led_status, battery_status, timemillis):
        self.operation = operation
        self.location_id = location_id
        self.supply_id = supply_id
        self.led_status = led_status
        self.battery_status = battery_status
        self.timemillis = timemillis

    def __str__(self):
        return "operation: %s, location_id: %s, supply_id: %s, led_status: %s, battery_status: %s, timemillis: %s" % (self.operation, self.location_id, self.led_status, self.battery_status, self.timemillis)


#################################
####                         ####
#################################
def calculate_packet_index_diff(sensor_address, packet_index, old_index):
    if (sensor_address in BLEEP_SENSORS):
        packet_index_diff = (packet_index - old_index)%(SENSOR_INDICES_NUM_ACTIVE)
    return packet_index_diff