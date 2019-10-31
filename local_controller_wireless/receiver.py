#!/usr/bin/env python
# -*- coding: utf-8 -*-

#sudo easy_install python-py
#sudo pip install requests

import sys
import json
import time
import urllib2
import commands
import re
import signal
import bluetooth._bluetooth as bluez
import BeaconScan
import os
import WIFIProbeScanner
from TMeshScan import TMeshScanner
from datetime import datetime
from threading import Thread, Condition


bleDevId = 0
ble_dev_ids = []
localMeshBeacons = []
localLocationBeacons = []
tagSensor = dict()
scansCounter = 0
jsonArray = []
condition = Condition()
sendCondition = Condition()
wifi = "wlan0"
kill_received = [False]
debug = False
global scanner
global producer
global consumer
global observer


folder = os.path.dirname(os.path.realpath(__file__))
f = open(folder + "/receiver.log","w")
f.write("starting...\n\n")
f.flush()

def signal_handler(signal, frame):
    global kill_received
    global observer
    global scanner
    global producer
    global consumer
    print "signal_handler receiver got SIGINT signal handler"
    kill_received[0] = True
    # observer.kill()
    scanner.kill()
    print "TmeshScan killed"
    sys.stdout.flush()
    # producer.join()
    # consumer.join()
    print "threads joined"
    sys.stdout.flush()
    time.sleep(2)
    sys.exit(0)


def miliseconds_to_datetime(timeMili):
    timeSec = timeMili/1000.0
    return datetime.fromtimestamp(timeSec).strftime('%Y-%m-%d %H:%M:%S.%f')


class WIFIProbeObserver:
    def __init__(self, scanner):
        scanner.register_observer(self)
 
    def notifySignals(self, wifi_probe_signals, given_sensor_name, interface):
        global jsonArray
        global condition
        condition.acquire()
        for data in wifi_probe_signals:
            sys.stdout.flush()
            jsonItem = { "account": account, "site_id": site_id, "site": site, "source": data.source, "type": receiver_type, "rssi": data.rssi, "sensor": given_sensor_name + "_" + interface, "timemili": int(data.timemili)}
            jsonArray.append(jsonItem)
        condition.notify()
        condition.release()

class TMeshObserver:
    def __init__(self, scanner):
        scanner.register_observer(self)
 
    def notifySignals(self, mesh_signals):
        global jsonArray
        global condition
        condition.acquire()
        for data in mesh_signals:
            # f.write("data:  %s\n" % str(data))
            # f.flush()
            if data.type == 'signal':
                jsonItem = { "account": account, "site_id": site_id, "site": site, "type": "mesh_signal", "sensor_name": data.sensor_name, "sensor_unit_id": data.sensor_unit_id, "sensor_data": data.sensor_data, "source_name": data.source_name, "source_unit_id": data.source_unit_id, "source_tag_id": data.source_tag_id, "timemili": int(data.timemillis)}
            elif data.type == 'aggregated_signal':
                jsonItem = { "account": account, "site_id": site_id, "site": site, "type": "aggregated_signal", "sensor_name": data.sensor_name, "sensor_unit_id": data.sensor_unit_id, "sensor_data": data.sensor_data, "source_name": data.source_name, "source_unit_id": data.source_unit_id, "source_tag_id": data.source_tag_id, "timemili": int(data.timemillis)}
            elif data.type == 'battery':
                jsonItem = {"type": "mesh_battery", "source_name": data.source_name, "source_unit_id": data.source_unit_id, "battery": data.battery, "timemili": int(data.timemillis)}
            elif data.type == 'accelerometer':
                jsonItem = {"type": "accelerometer", "sensor_address": data.sensor_address, "timemili": int(data.timemillis), "sensor_unit_id": data.sensor_unit_id} #"x_move": data.x_move, "y_move": data.y_move, "z_move": data.z_move}}
                if (data.count != -1):
                    jsonItem["count"] = data.count

            # Passive signals do not contain packet_recognition key - add it only to active signals
            if hasattr(data, "signal_identifier"):
                jsonItem["signal_identifier"] = data.signal_identifier
            jsonArray.append(jsonItem)
        condition.notify()
        condition.release()

    def notifyAlerts(self, mesh_alerts):
        #f.write("notifyAlerts started: mesh_alerts is %s" % str(mesh_alerts))
        # f.flush()
        jsonAlertArray = []
        for data in mesh_alerts:
            jsonAlertItem = { "account": account, "site_id": site_id, "site": site, "source": data.source_name, "type": "mesh_alert", "level": int(float(data.rssi)), "sensor": data.sensor_name, "timemili": int(data.timemillis), "signal_identifier": data.signal_identifier}
            if hasattr(data, "tx_power"):
                jsonAlertItem["tx_power"] = data.tx_power
            jsonAlertArray.append(jsonAlertItem)
        res = send_results(jsonAlertArray)
        if res is not None:
            if (res.getcode() == 413):
                print "too much data to send (413): ", len(data)
                print "data is going to be lost, since we clean it"
                sys.stdout.flush()
        else:
            #sever is down? don't keep results
            print "Sleep before trying to send again (10 secs)."
            sys.stdout.flush()
            time.sleep(10)

    def notifyBattery(self, battery_data):
        jsonBatteryArray = []
        for data in battery_data:
            jsonBatteryItem = {"type": "battery_status", "name": data.name, "battery": data.battery, "timemili": int(data.timemillis)}            
            jsonBatteryArray.append(jsonBatteryItem)

        data = '{"data":' + json.dumps(jsonBatteryArray) + '}'
        url = "http://localhost/maintenance/battery"
        
        req = urllib2.Request(url)
        req.add_data(data)
        req.add_header('Content-Type', 'application/json; charset=utf-8')
        try:
            res = urllib2.urlopen(req)
        except urllib2.HTTPError, err:
            if err.code == 404:
               print "Page not found! (for maintenance)"
            elif err.code == 403:
               print "Access denied! (for maintenance)"
            else:
               print "Something happened! Error code", err.code
            sys.stdout.flush()
            res = None
        except urllib2.URLError, err:
            res = None
            print "Failed sending maintenance data to cloud. Server is down? ", err.reason
            sys.stdout.flush()

    # def notifySupply(self, supply_info):
    #     jsonSupplyArray = []        
    #     jsonSupplyItem = { "account": account, "site_id": site_id, "site": site, "type": "mesh_supply", "operation": supply_info.operation, "location_id": supply_info.location_id, "supply_id": supply_info.supply_id, "led_status": supply_info.led_status, "battery_status": supply_info.battery_status}
    #     jsonSupplyArray.append(jsonSupplyItem)
    #     res = send_results(jsonSupplyArray)
    #     if res is not None:
    #         if (res.getcode() == 413):
    #             print "too much data to send (413): ", len(data)
    #             print "data is going to be lost, since we clean it"
    #             sys.stdout.flush()
    #     else:
    #         #sever is down? don't keep results
    #         print "Sleep before trying to send again (10 secs)."
    #         sys.stdout.flush()
    #         time.sleep(10)

    def notifyHardwareParameters(self, params):
        # print "XXXX notifyHardwareParameters, params = " + str(params)        
        data = '{"data":' + json.dumps(params) + '}'
        url = "http://localhost/maintenance/watchDogHardwareParameters"
        req = urllib2.Request(url)
        req.add_data(data)
        req.add_header('Content-Type', 'application/json; charset=utf-8')
        try:
            res = urllib2.urlopen(req)
        except urllib2.HTTPError, err:
            if err.code == 404:
               print "Page not found! (for maintenance)"
            elif err.code == 403:
               print "Access denied! (for maintenance)"
            else:
               print "Something happened! Error code", err.code
            sys.stdout.flush()
            res = None
        except urllib2.URLError, err:
            res = None
            print "Failed sending maintenance data to cloud. Server is down? ", err.reason
            sys.stdout.flush()        

    def notifyMaintenance(self, maint_data):
        # print "XXXX ["+ str(miliseconds_to_datetime(maint_data["time"])) +" ] notifyMaintenance, maint_data = " + str(maint_data)        
        data = '{"data":' + json.dumps(maint_data) + '}'
        url = "http://localhost/maintenance"
        req = urllib2.Request(url)
        req.add_data(data)
        req.add_header('Content-Type', 'application/json; charset=utf-8')
        try:
            res = urllib2.urlopen(req)
        except urllib2.HTTPError, err:
            if err.code == 404:
               print "Page not found! (for maintenance)"
            elif err.code == 403:
               print "Access denied! (for maintenance)"
            else:
               print "Something happened! Error code", err.code
            sys.stdout.flush()
            res = None
        except urllib2.URLError, err:
            res = None
            print "Failed sending maintenance data to cloud. Server is down? ", err.reason
            sys.stdout.flush()

    # def kill(self):
    #     global condition
    #     condition.notify()
    #     condition.release()



class unitServerCommandsThread(Thread):
    def run(self):
        global kill_received
        while not kill_received[0]:
            # handle data from unit-server
            packet = sys.stdin.readline()
            while (packet):
                if scanner:
                    print "Received New command from unit-server... " + str(packet)          
                    packet_json = json.loads(str(packet))
                    command = packet_json["command"]
                    # Open TMeshscan LOGGER
                    if command == 'open_ble_logger':
                        scanner.open_ble_logger()
                    # Close TMeshscan LOGGER        
                    elif command == 'close_ble_logger':
                        scanner.close_ble_logger()    
                    packet = sys.stdin.readline()           
        print "killed unit-server commands thread"
        sys.stdout.flush()



class ScanThread(Thread):
    def run(self):
        global receiver_type
        global kill_received
        global condition
        global observer
        if ((receiver_type != "bleep") and (receiver_type != "wifi_monitor")):
            while not kill_received[0]:
                condition.acquire()
                scan()
                #print "#data: ", len(jsonArray)
                #sys.stdout.flush()
                condition.notify()
                condition.release()
                time.sleep(2.5)
            print "killed scan thread (producer)"
        elif (receiver_type == "wifi_monitor"):
            global scanner
            global wifi_monitor_device_list
            if (len(wifi_monitor_device_list) > 0):
                wifi_probes_signals = WIFIProbeScanner.probeScanner()
                observer = WIFIProbeObserver(wifi_probes_signals)
                for wifi_interface in wifi_monitor_device_list:
                    print ("starting WIFI-probe on " + wifi_interface)
                    wifi_probes_signals.startProbeScanner(str(wifi_interface), receiverName)
                sys.stdout.flush()
                while not kill_received[0]:
                    time.sleep(2)
                print "killed scan thread (producer)"
            else:
                print "no device is listed to be wifi-monitor. wifi-monitor will not collect data."
            sys.stdout.flush()
        elif (receiver_type == "bleep"):
            global scanner
            global ble_dev_ids
            global localMeshBeacons
            global localLocationBeacons
            global tagSensor
            scanner = TMeshScanner(ble_dev_ids,localMeshBeacons,localLocationBeacons,tagSensor,10)
            observer = TMeshObserver(scanner)
            while not kill_received[0]:
                time.sleep(2)
            print "killed scan thread (producer)"
            sys.stdout.flush()

class SendThread(Thread):
    def run(self):
        global jsonArray
        global condition
        data = []
        global kill_received
        while not kill_received[0]:
            #print "(consumer)"
            #sys.stdout.flush()
            condition.acquire()
            while not jsonArray:
                condition.wait()
            
            sendCondition.acquire()
            data.extend(jsonArray)
            sendCondition.release()
            
            jsonArray = []
            condition.notify()
            condition.release()

            sendCondition.acquire()
            if (len(data) > 0):
                res = send_results(data)
                # f.write(json.dumps(data) + "\n")
                # f.flush()
                if res is not None:
                    if (res.getcode() == 200):
                        #print "sent #data: ", len(data)
                        #sys.stdout.flush()
                        data = []
                    elif (res.getcode() == 413):
                        print "too much data to send (413): ", len(data)
                        print "data is going to be lost, since we clean it"
                        sys.stdout.flush()
                        data = []
                else:
                    #sever is down? don't keep results
                    print "Sleep before trying to send again (10 secs)."
                    sys.stdout.flush()
                    time.sleep(10)
                    data = []
            sendCondition.release()
            
            time.sleep(0.5)

        print "killed send thread (consumer)"
        sys.stdout.flush()


def scan():
    #print "scanning!"
    #sys.stdout.flush()
    global jsonArray
    global receiver_type
    now = datetime.utcnow()
    millis = unix_time_millis(now)
    fullstr = now.strftime("%Y-%m-%dT%H:%M:%S.%f%z")

    if receiver_type == "wifi":
        global localAps,wifi,freqs,threshold
        aps = scanWifiAPs(wifi,freqs)
        for ap in aps:
            if (ap.lower() != apName) and ap in localAps: # or if float(aps[ap]) > threshold:
                jsonItem = { "account": account, "site_id": site_id, "site": site, "source": ap, "type": receiver_type, "level": int(float(aps[ap])), "sensor": receiverName, "timemili": int(millis)}
                jsonArray.append(jsonItem)
    elif receiver_type == "bluetooth":
        global bluetooth_name, bluetooth_address
        rssi = scanBluetooth(bluetooth_address)
        if rssi is not None:
            jsonItem = { "account": account, "site_id": site_id, "site": site, "source": bluetooth_name, "type": receiver_type, "level": int(float(rssi)), "sensor": receiverName, "timemili": int(millis)}
            #print jsonItem
            jsonArray.append(jsonItem)
    elif receiver_type == "bluetooth_monitor":
        sys.stdout.flush()
        data = scanBluetoothMonitor()
        for source in data:
            jsonItem = { "account": account, "site_id": site_id, "site": site, "source": { "address": source["address"], "name": source["name"] }, "type": receiver_type, "sensor": receiverName, "timemili": int(millis)}
            #print jsonItem
            jsonArray.append(jsonItem)
    elif receiver_type == "beacon":
        global localBeacons
        beacons = BeaconScan.parse_events(sock, 10)
        for beacon in beacons:
            if beacon.name in localBeacons:
                jsonItem = { "account": account, "site_id": site_id, "site": site, "source": beacon.name, "type": receiver_type, "level": int(float(beacon.rssi[0])), "sensor": receiverName, "timemili": int(beacon.timemillis)}
                #print jsonItem
                jsonArray.append(jsonItem)

def scanBluetoothMonitor():
    global receiver_dev
    sys.stdout.flush()
    scan_data = []
    data = commands.getoutput("sudo hcitool -i hci" + receiver_dev + " scan --flush")
    pattern = re.compile(r'(([0-9A-F]{2}[:-]){5}([0-9A-F]{2}))\t(.*)')
    it = pattern.finditer(data)
    for match in it:
        #print str(match.group(1)) + " " + str(match.group(4))
        scan_data.append({ "name": match.group(4), "address": match.group(1) })

    if (len(scan_data) > 0):
        print "found " + str(len(scan_data)) + " bluetooth objects"
        sys.stdout.flush()
                         
    return scan_data

def scanBluetooth(address):
    data = commands.getoutput("sudo hcitool rssi " + address)
    pattern = re.compile(r'RSSI return value: (-?\d+)')
    results = pattern.findall(data)
    if (len(results) > 0):
        return results[0]
    else:
        return None

def scanWifiAPs(wifi, freqs):
    data = commands.getoutput("sudo iw dev " + wifi + " scan freq " + freqs)
    pattern = re.compile(r'SSID: ([A-Za-z0-9\t ._+-]+)|(-\d+(?:\.\d+)?) dBm')
    results = pattern.findall(data)
    aps = {}
    i = 0
    while i < len(results):
        if (len(results) > i+1):
            aps[results[i+1][0]] = results[i][1]
        else:
            print "results = " + str(len(results)) + "    i = " + str(i)
        
        i += 2
    
    return aps

def send_results(data):
    global debug
    postTime = unix_time_millis(datetime.utcnow());
    data = '{"receiverTime":' + str(postTime) + ', "receiver": "' + receiverName + '", "data":' + json.dumps(data) + '}'
    #print data
    url = "http://localhost/signal"

    # if (debug):
    #     url = "http://"+serverIp+":"+serverPort+"/signal"
        
    req = urllib2.Request(url)
    req.add_data(data)
    req.add_header('Content-Type', 'application/json; charset=utf-8')
    try:
        res = urllib2.urlopen(req)
    except urllib2.HTTPError, err:
       if err.code == 404:
           print "Page not found!"
       elif err.code == 403:
           print "Access denied!"
       else:
           print "Something happened! Error code", err.code
       sys.stdout.flush()
       res = None
    except urllib2.URLError, err:
        res = None
        print "Failed sending data to cloud. Server is down? ", err.reason
        sys.stdout.flush()
    return res

def unix_time_millis(dt):
    epoch = datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds() * 1000.0

def scanWifiFrequency(wifi,threshold):
    data = commands.getoutput("sudo iw dev " + wifi + " scan")
    pattern = re.compile(r'SSID: ([A-Za-z0-9\t ._+-]+)|(-\d+(?:\.\d+)?) dBm|freq: (\d+)')
    results = pattern.findall(data)
    freqs = {}
    i = 0
    print "threshold:" + str(threshold)
    while (i+2 < len(results)):
        if (results[i+2][0].lower() != apName) and results[i+2][0] in localAps: #or float(results[i+1][1]) > threshold:
            if results[i][2] in freqs:
                freqs[results[i][2]].append(results[i+2][0])
            else:
                freqs[results[i][2]] = [ results[i+2][0] ] 
        i += 3

    query_freqs = ""  
    for freq in freqs :
        query_freqs += freq + " "

    print "freqs:" + query_freqs
    return query_freqs[:-1]

print "receiver.py is starting... with parameters : "    
print sys.argv[1:]

if len(sys.argv) > 2 and sys.argv[1] == 'auto':
    #print "(automatic start)..."
    receiver_type = sys.argv[2]
    if receiver_type == 'wifi':
        wifi = sys.argv[3]
        first_param_index = 4
    elif receiver_type == 'bluetooth':
        first_param_index = 3
    elif receiver_type == 'bluetooth_monitor':
        first_param_index = 3
    elif receiver_type == 'beacon':
        bleDevId = int(sys.argv[3])
        first_param_index = 4
    elif receiver_type == 'bleep':
        first_param_index = 3
    elif receiver_type == 'wifi_monitor':
        first_param_index = 4
        wifi_monitor_device_list = json.loads(sys.argv[3])
    
    serverIp = sys.argv[first_param_index]
    serverPort = sys.argv[first_param_index+1]
    account = sys.argv[first_param_index+2]
    site_id = sys.argv[first_param_index+3]
    site = sys.argv[first_param_index+4]
    mode = sys.argv[first_param_index+5]
    debug = True if mode == 'debug' else False
    sensor = json.loads(sys.argv[first_param_index+6])
    receiverName = sensor["name"]
    if receiver_type == 'wifi':
        threshold = sensor["threshold"]
        localAps = json.loads(sys.argv[first_param_index+7])
        apName = sys.argv[first_param_index+8].lower()
    elif receiver_type == 'bluetooth':
        receiver_address = sensor["address"]
        blueteeth = json.loads(sys.argv[first_param_index+7])#unit["blueteeth"]
        bluetooth_address = sys.argv[first_param_index+8]
        for bluetooth in blueteeth:
            if bluetooth.get("address") == bluetooth_address:
                bluetooth_name = bluetooth.get("name")
                break
    elif receiver_type == 'bluetooth_monitor':
        receiver_address = sensor["address"]
        receiver_dev = str(sensor["dev"])
    elif receiver_type == 'beacon':
        localBeacons = json.loads(sys.argv[first_param_index+7])
        try:
            sock = bluez.hci_open_dev(bleDevId)
            print "ble thread started"

        except:
            print "error accessing bluetooth device..."
            sys.exit(1)

        BeaconScan.hci_le_set_scan_parameters(sock)
        BeaconScan.hci_enable_le_scan(sock)
    elif receiver_type == 'bleep':
        # read dev ids from sensor
        localMeshBeacons = json.loads(sys.argv[first_param_index+7])
        localLocationBeacons = json.loads(sys.argv[first_param_index+8])
        scannerMaintenanceInterval = json.loads(sys.argv[first_param_index+9])
        for sensorItem in sensor["sensors"]:
            ble_dev_ids.append(sensorItem["dev"])
        if "tag_sensors" in sensor:
            tagSensor["enable"] = True
            tagSensor["mac_address"] = sensor["tag_sensors"][0]["address"]
        else:
            tagSensor["enable"] = False

    elif receiver_type == 'wifi_monitor':
        receiver_dev = sensor["dev"]
else:
    print "Enter receiver type:"
    receiver_type = sys.stdin.readline().rstrip()
    print "Enter account:"
    account = sys.stdin.readline().rstrip()
    print "Enter site ID:"
    site_id = sys.stdin.readline().rstrip()    
    print "Enter site:"
    site = sys.stdin.readline().rstrip()
    print "Enter receiver name:"
    receiverName = sys.stdin.readline().rstrip()
    print "Enter cloud ip:"
    serverIp = sys.stdin.readline().rstrip()
    print "Enter cloud port:"
    serverPort = sys.stdin.readline().rstrip()
    # DEBUG
    unit =json.loads('{"_id":"54eb1995fee1085c2a550ed9","serial":"00000000dff3456e","update":300,"network":"OUT1_AP","name":"OUT1","accesspoint":{"name":"OUT1_AP","enabled":false},"sensors":{"wifi":[{"name":"OUT1_S0","threshold":-82,"frequency":4,"enabled":false}],"bluetooth":[{"name":"OUT1_BLE1","address":"00:1A:7D:DA:71:0F","dev":"1","dev_ver":"4.0","enabled":false},{"name":"OUT1_BLE0","address":"00:1A:7D:DA:71:0D","dev":"0","dev_ver":"4.0","enabled":false}],"beacon":[{"name":"OUT1_BLE1_BC","address":"00:1A:7D:DA:71:0F","dev":"1","dev_ver":"4.0","enabled":false},{"name":"OUT1_BLE0_BC","address":"00:1A:7D:DA:71:0D","dev":"0","dev_ver":"4.0","enabled":false}]},"account": "Iddo", site":{"name":"outdoor","beacons":["100-1","100-2","100-3"],"bleeps":[{"address":"bc:6a:29:aa:f9:19","name":"mesh1"},{"address":"78:a5:04:29:02:f9","name":"mesh2"}],"local_aps":["OUT1_AP"],"blueteeth":[{"address":"00:1A:7D:DA:71:0F","name":"OUT1_BLE1"},{"address":"00:1A:7D:DA:71:0D","name":"OUT1_BLE0"}]}}')
    if receiver_type == 'wifi':
        receiverName = str(unit["sensor"]["name"])        
        threshold = unit["sensor"]["threshold"]
        localAps = unit["site"]["local_aps"]
        apName = unit["accesspoint"]["name"].lower()
    elif receiver_type == 'bleep':
        receiverName = str(unit["sensors"]["bluetooth"][0]["name"])
        receiver_address = unit["sensors"]["bluetooth"][0]["address"]
        try:
            sock = bluez.hci_open_dev(bleDevId)
            print "ble thread started"

        except:
            print "error accessing bluetooth device..."
            sys.exit(1)

        TMeshScan.hci_le_set_scan_parameters(sock)
        TMeshScan.hci_enable_le_scan(sock)
    else:
        receiverName = str(unit["bluetooth"]["name"])
        receiver_address = unit["bluetooth"]["address"]
        blueteeth = unit["site"]["blueteeth"]
        #DEBUG
        bluetooth_address = "00:1A:7D:0A:B6:B0"
        for bluetooth in blueteeth:
            if bluetooth.get("address") == bluetooth_address:
                bluetooth_name = bluetooth.get("name")
                break

#print "data: " + receiver_type + ":" + account + ":" + site + ":" + receiverName + ":" + serverIp + ":" + serverPort
if receiver_type == 'wifi':
    print "Loading frequencies..."
    sys.stdout.flush()
    freqs = scanWifiFrequency(wifi,threshold)
    while (freqs == ""):
        print "no frequencies found (sleeping 5 secs)"
        sys.stdout.flush()
        time.sleep(5)
        freqs = scanWifiFrequency(wifi,threshold)

signal.signal(signal.SIGINT, signal_handler)
producer = ScanThread()
producer.start()
consumer = SendThread()
consumer.start()
unitServerCommands = unitServerCommandsThread()
unitServerCommands.start()
            

try:
    while True:                    
        # sys.stdout.flush()
        time.sleep(10)
except KeyboardInterrupt:
    global scanner
    kill_received[0] = True
    print "KeyboardInterrupt receiver got SIGINT signal"
    sys.stdout.flush()
    scanner.kill()
    producer.join()
    consumer.join()
    print "threads successfully closed"
    sys.stdout.flush()