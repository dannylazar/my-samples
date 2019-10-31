#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import json
import time
# from datetime import datetime
import commands
import math
import logging

# Log configuration :
FORMAT = '%(asctime)-15s %(message)s'
folder = os.path.dirname(os.path.realpath(__file__))

# define log levels
LOW_LOG_LEVEL = 17
MEDIUM_LOG_LEVEL = 16
HIGH_LOG_LEVEL = 15


LOW_VERBOSITY = LOW_LOG_LEVEL + 1
MEDIUM_VERBOSITY = MEDIUM_LOG_LEVEL + 1
HIGH_VERBOSITY = HIGH_LOG_LEVEL + 1

# logging.basicConfig(format=FORMAT, filename=(folder + "/bleAdvertiser_" + str(datetime.utcnow()) + ".log"), filemode='w')
logging.basicConfig(format=FORMAT, filename=(folder + "/bleAdvertiser.log"), filemode='w')
logger = logging.getLogger('bleAdvertiser')
logger.setLevel(LOW_LOG_LEVEL);
logger.info("bleAdvertiser Started... (LOG_LEVEL is " + str(MEDIUM_VERBOSITY) + ')')


BLE_PACKET_SIZE_NIBBLES = 62
BLE_PACKET_SIZE_BYTES = 31


def ble_start_advertising(dev_id, data):
	data_length_nibbles = len(data)
	data_length_bytes = int(math.ceil(data_length_nibbles/2.0))
	logger.log(LOW_VERBOSITY,'ble_start_advertising: dev_id %s, should advertise %s Bytes (%s valid nibbles), data is: %s' % (dev_id, str(data_length_bytes), str(data_length_nibbles), str(data)))
	# pad with zeroes
	while data_length_nibbles < BLE_PACKET_SIZE_NIBBLES:
		data += '0'
		data_length_nibbles += 1

	set_advertising_data_string = "sudo hcitool -i hci" + str(dev_id) + " cmd 0x08 0x0008 " + str(format(data_length_bytes, 'x'))

	# add spaces between advertisment data bytes
	for i in range(0,BLE_PACKET_SIZE_NIBBLES-1):
		if (i%2 == 0):
			set_advertising_data_string += ' '
		set_advertising_data_string += data[i]
	logger.log(HIGH_VERBOSITY,'ble_start_advertising: dev_id %s, set_advertising_data_string is: %s' % (dev_id, set_advertising_data_string))
	
	# LE_Set_Advertising_Data command
	logger.log(LOW_VERBOSITY,'ble_start_advertising: dev_id %s, Issue LE_Set_Advertising_Data command' % (dev_id))	
	status = commands.getoutput(set_advertising_data_string)
	logger.log(MEDIUM_VERBOSITY,'ble_start_advertising: dev_id %s, LE Set Advertising Data status: %s' % (dev_id, status))	
	event_code_index = status.index('HCI Event: 0x')+len('HCI Event: 0x')
	event_code = status[event_code_index:event_code_index+2]
	if event_code == '0e':
		error_code = status[-2:]
		if int(error_code, 16) == 0:
			logger.log(LOW_VERBOSITY,'ble_start_advertising: dev_id %s - LE Set Advertising Data completed succesfully (error code is %s)' % (dev_id, error_code))
		else:
			logger.log(LOW_VERBOSITY,'ble_start_advertising: dev_id %s -  LE Set Advertising Data did NOT complete succesfully (error code is %s)' % (dev_id, error_code))		
			sys.stdout.write('fail')
			sys.stdout.flush()
			return	

	# LE_Set_Advertise_Enable (start)
	logger.log(LOW_VERBOSITY,'ble_start_advertising: dev_id %s, Issue LE_Set_Advertise_Enable (start) command' % (dev_id))	
	start_advertisement_string = "sudo hcitool -i hci" + str(dev_id) + " cmd 0x08 0x000a 01"
	logger.log(HIGH_VERBOSITY,'ble_start_advertising: dev_id %s, start_advertisement_string is: %s' % (dev_id, start_advertisement_string))	
	status = commands.getoutput(start_advertisement_string)
	logger.log(MEDIUM_VERBOSITY,'ble_start_advertising: dev_id %s, LE Start Advertising status: %s' % (dev_id, status))	
	event_code_index = status.index('HCI Event: 0x')+len('HCI Event: 0x')
	event_code = status[event_code_index:event_code_index+2]
	if event_code == '0e':	
		error_code = status[-2:]	
		if int(error_code, 16) == 0:
			logger.log(LOW_VERBOSITY,'ble_start_advertising: dev_id %s - function completed succesfully' % (dev_id))	
		else:
			logger.log(LOW_VERBOSITY,'ble_start_advertising: dev_id %s - function did NOT complete succesfully, error code is %s' % (dev_id, error_code))		
			sys.stdout.write('fail')
			sys.stdout.flush()
			return
	sys.stdout.write('success')
	sys.stdout.flush()	



def ble_stop_advertising(dev_id):
	logger.log(LOW_VERBOSITY,'ble_stop_advertising: dev_id %s, Issue LE_Set_Advertise_Enable (stop) command' % (dev_id))		
	stop_advertisement_command_string = "sudo hcitool -i hci" + str(dev_id) + " cmd 0x08 0x000a 00"
	logger.log(HIGH_VERBOSITY,'ble_stop_advertising: dev_id %s, stop_advertisement_command_string is: %s' % (dev_id, stop_advertisement_command_string))		
	# LE_Set_Advertise_Enable (stop)
	status = commands.getoutput(stop_advertisement_command_string)
	logger.log(MEDIUM_VERBOSITY,'ble_stop_advertising: dev_id %s, LE Stop Advertising status: %s' % (dev_id, status))	
	event_code_index = status.index('HCI Event: 0x')+len('HCI Event: 0x')
	event_code = status[event_code_index:event_code_index+2]
	if event_code == '0e':	
		error_code = status[-2:]	
		if int(error_code, 16) == 0:
			logger.log(LOW_VERBOSITY,'ble_stop_advertising: dev_id %s - function completed succesfully' % (dev_id))
		else:
			logger.log(LOW_VERBOSITY,'ble_stop_advertising: dev_id %s - function did NOT complete succesfully, error code is %s' % (dev_id, error_code))
			sys.stdout.write('fail')
			sys.stdout.flush()
			return
	sys.stdout.write('success')
	sys.stdout.flush()			


meshBleDevices = sys.argv[-1]
logger.log(LOW_VERBOSITY,'Initialize BLE: Print all BLE Devices: %s' % (str(meshBleDevices)))
for dev_id in meshBleDevices:
	# LE_Set_Advertising_Parameters command:
	# A0 00 A0 00  min/max adv. interval is 100ms
	# 03           non-connectable undirected advertising
	# 00           own address is public
	# 00           target address is public (not used for undirected advertising)
	# 00 00 00 ... target address (not used for undirected advertising)
	# 07           adv. channel map (enable all)
	# 00           filter policy (allow any)			
	set_advertising_parameters_string = "sudo hcitool -i hci" + str(dev_id) + " cmd 0x08 0x0006 A0 00 A0 00 03 00 00 00 00 00 00 00 00 07 00"
	logger.log(HIGH_VERBOSITY,'Initialize BLE: dev_id %s, set_advertising_parameters_string is: %s' % (dev_id, set_advertising_parameters_string))
	logger.log(LOW_VERBOSITY,'Initialize BLE: dev_id %s, Issue LE Set Advertisement Parameters command' % (dev_id))
	status = commands.getoutput(set_advertising_parameters_string)
	logger.log(HIGH_VERBOSITY,'Initialize BLE: dev_id %s, Set Advertisement Parameters status: %s' % (dev_id, status))	
	event_code_index = status.index('HCI Event: 0x')+len('HCI Event: 0x')
	event_code = status[event_code_index:event_code_index+2]
	if event_code == '0e':			
		error_code = status[-2:]			
		if int(error_code, 16) == 0:
			logger.log(LOW_VERBOSITY,'Initialize BLE: dev_id %s, LE Set Advertising Parameters completed succesfully' % (dev_id))
		else:		
			logger.log(LOW_VERBOSITY,'Initialize BLE: dev_id %s, LE Set Advertising Parameters did NOT complete succesfully (error_code is %s)' % (dev_id, error_code))


# handle data from unit-server
packet = sys.stdin.readline()
while (packet):
	logger.log(LOW_VERBOSITY,'\n\nReceived New System Packet To Advertise... %s' % (str(packet)))			
	packet_json = json.loads(str(packet))
	logger.log(HIGH_VERBOSITY,'packet_json is %s' % (str(packet_json)))	
	dev_id = packet_json["dev_id"]
	command = packet_json["command"]
	# Start Advertising
	if command == 'start':
		data = packet_json["data"]
		ble_start_advertising(dev_id, data)
	# Stop Advertising
	elif command == 'stop':
		ble_stop_advertising(dev_id)
	packet = sys.stdin.readline()