from scapy.all import *
from datetime import datetime

PROBE_REQUEST_TYPE=0
PROBE_REQUEST_SUBTYPE=4
jsonArray = []

class wifiProbeData():
    def __init__(self, source, target, sensor, SSID, rssi, timemili):
        self.source = source;
        self.target = target;
        self.sensor = sensor;
        self.SSID = SSID;
        self.rssi = rssi;
        self.timemili = timemili;

class probeScanner():
    def __init__(self):
        self.observers = []

    def register_observer(self, observer):
        self.observers.append(observer)

    def notify_observers_signals(self, signals, given_sensor_name, interface):
        for observer in self.observers:
            observer.notifySignals(signals, given_sensor_name, interface)

    def startProbeScanner(self,interface,given_sensor_name):
        global jsonArray
        while True:
            sniff(iface=interface,prn=PacketHandler,count=5)
            if (len(jsonArray) > 0):
                self.notify_observers_signals(jsonArray, given_sensor_name, interface)
                jsonArray = [];
                sys.stdout.flush()            

def unix_time_millis(dt):
    epoch = datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds() * 1000.0

def PacketHandler(pkt):
    if pkt.haslayer(Dot11):
        if pkt.type==PROBE_REQUEST_TYPE and pkt.subtype == PROBE_REQUEST_SUBTYPE:
            now = datetime.utcnow()
            now = unix_time_millis(now);
            global jsonArray
            try:
                extra = pkt.notdecoded
            except:
                extra = None
            if extra!=None:
                signal_strength = -(256-ord(extra[-4:-3]))
            else:
                signal_strength = -100
            if (not pkt.getlayer(Dot11ProbeReq).info):
                SSID = "undefined"
            else:
                SSID = pkt.getlayer(Dot11ProbeReq).info
            jsonArray.append(wifiProbeData(pkt.addr2, pkt.addr3, "temp_remove", SSID, signal_strength, now));