'''
Created on Oct 12, 2012

@author: cgrubb
'''
from json import loads, dumps

from zmq import Context, SUB, PUB, SUBSCRIBE
from zmq.eventloop import zmqstream, ioloop
ioloop.install()
import socket
import sys
import threading

PORT = 45681
NETWORK = "192.168.10.255"
MAX = 1024

ANY = "0.0.0.0"
LOCAL = "127.0.0.1"
MCAST_ADDR = "224.168.2.9"
MCAST_PORT = 1600
SETTINGS_URL = "inproc://settings"

class Listener(threading.Thread):
    
    def __init__(self, context, settings_url, tags,
                 interface = "127.0.0.1",
                 mcast_addr = "224.168.2.9",
                 mcast_port = 1600):
        threading.Thread.__init__(self)
        self.context = context or Context().instance()
        self.settings = self.context.socket(PUB)
        self.settings.bind(settings_url)
        self.register = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.register.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tags = tags
        self.interface = interface
        self.mcast_addr = mcast_addr
        self.mcast_port = mcast_port
        self.daemon = True
        
    def run(self):
        self.register.bind((self.interface, self.mcast_port))
        self.register.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 255)
        self.register.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, 
                                 socket.inet_aton(self.mcast_addr) + socket.inet_aton(self.interface))
        while True:
            try:
                data, address = self.register.recvfrom(MAX)
                print data, address
                msg = loads(data)
                for item in msg:
                    if item['tag'] in self.tags:
                        self.settings.send(dumps(item))    
            except Exception as e:
                print e.message

                
def SettingsListener(tags,
                     interface = "127.0.0.1",
                     mcast_addr = "224.168.2.9",
                     mcast_port = 1600):
    
    def decorator(cls):
        class _wrapper_():
            def __init__(self, *args, **kwargs):
                self.listener = Listener(None, "inproc://settings",                                         
                                         tags,
                                         interface = interface,
                                         mcast_addr = mcast_addr,
                                         mcast_port = mcast_port)
                self.listener.start()
                self.cls = cls(*args, **kwargs)
                                
            def __getattr__(self, *args):
                return getattr(self.cls, *args)
        return _wrapper_    
    return decorator

tags = []        
if len(sys.argv) > 1 and sys.argv[1] != '':
    for i in range(1, len(sys.argv)):
        tags.append(sys.argv[i])


@SettingsListener(set(tags),
                  interface="192.168.10.32")
class Worker():
    
    def __init__(self, register_url, context, limit = 5):
        self.context = context or Context().instance()
        self.ioloop = ioloop.IOLoop.instance()
        self.register = context.socket(SUB)
        self.register.connect(register_url)
        self.register.setsockopt(SUBSCRIBE,'')
        self.register_stream = zmqstream.ZMQStream(self.register, self.ioloop)
        self.register_stream.on_recv(self._register_ok)
        self._subscribe_setup()
        self.limit = limit
        ioloop.PeriodicCallback(self._check_connections, 10000, self.ioloop).start()
    
    def _on_subscribe(self, message):
        print message
    
    def _subscribe_setup(self):
        self.subscribe = self.context.socket(SUB)
        self.sub_stream = zmqstream.ZMQStream(self.subscribe)
        self.sub_stream.on_recv(self._on_subscribe) 
        self.subscriptions = set([])
        
    
    def _check_connections(self):
        if len(self.subscriptions) > self.limit:
            print "limit exceeded, resetting..."
            self.sub_stream.close()
            self.subscribe.close()
            self._subscribe_setup()
    
    def _register_ok(self, message):
        print message
        msg = loads(message[0])        
        if msg["type"] == "error":
            print msg
        else:
            addr = msg["publish_address"]
            if not addr in self.subscriptions:
                self.subscriptions.add(addr)
                self.subscribe.connect(addr)
                self.subscribe.setsockopt(SUBSCRIBE, msg["topic"])
    
    def start(self):
        self.ioloop.start()
        
def main(tags):
    context = Context().instance()    
    w = Worker(SETTINGS_URL, context)
    w.start()

if __name__ == "__main__":            
    main(tags)