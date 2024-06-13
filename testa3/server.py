import sys
import select
import json
import socket
import time
import random
import uuid

WELL_KNOWN_HOSTS = [
    'owl.cs.umanitoba.ca',
    'eagle.cs.umanitoba.ca',
    'hawk.cs.umanitoba.ca',
    'osprey.cs.umanitoba.ca'
]

class Event:
    def __init__(self, id, name, expiry):
        self.id = id
        self.name = name
        self.expiry = expiry
        
    def renew(self, expiry):
        self.expiry = expiry

class Peer:
    def __init__(self, host, port, name):
        self.host = host
        self.port = port
        self.name = name
        self.expiry = time.time() + 120
    
    def renew(self):
        self.expiry = time.time() + 120
        
    def info(self):
        return f"{self.host}:{self.port} - {self.name}"

class Server:
    def __init__(self, specified_port=None):
        self.gossips_received = []
        self.peers = {}
        self.add_well_known_hosts()
        self.words = ['', '', '', '', '']
        self.events = {}
        e = Event(str(uuid.uuid4()), 'gossip', time.time() + 60)
        self.events[e.id] = e
        
        self.host = None
        self.client_port = 15000 if specified_port == 16000 else None
        self.peer_port = 16000 if specified_port == 16000 else None

    def add_well_known_hosts(self):
        for host in WELL_KNOWN_HOSTS:
            key = f"{host}:16000"
            self.peers[key] = Peer(host, 16000, 'WK')
    
    def create_sockets(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.config_socket(client_socket, self.client_port)
        self.config_socket(peer_socket, self.peer_port)
        client_socket.listen(5)
        self.set_server_info(client_socket, peer_socket)
        self.log_sockets_info()
        return (client_socket, peer_socket)
    
    def set_server_info(self, client_socket, peer_socket):
        self.host = socket.gethostname()
        self.client_port = client_socket.getsockname()[1]
        self.peer_port = peer_socket.getsockname()[1]
    
    def log_sockets_info(self):
        print(f"Client | TCP | Port {self.client_port} | Host {self.host}")
        print(f"Peer   | UDP | Port {self.peer_port} | Host {self.host}")
         
    def config_socket(self, s, port=None):
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setblocking(False)
        if port:
            s.bind(('', port))
        else:
            s.bind(('', 0))
        
    def construct_peer(self, gossip):
        return Peer(gossip['host'], gossip['port'], gossip['name'])
    
    def next_event(self):
        return min(self.events.values(), key = lambda e: e.expiry)
    
    def clear_expired_peers(self):
        keys = []
        for key, value in self.peers.items():
            if value.expiry < int(time.time()):
                keys.append(key)
        for key in keys:
            del self.peers[key]
            print(f'Cleared peer {key}')

    def start(self):
        client_socket, peer_socket = self.create_sockets()
        
        inputs = [client_socket, peer_socket]
        clients = []
        outputs = []
        
        with client_socket, peer_socket:
            while True:
                try:
                    event = self.next_event()
                    timeout = event.expiry - time.time()
                            
                    readable, writable, exceptional = select.select(
                        inputs + clients,
                        outputs,
                        inputs + clients,
                        timeout if timeout > 0 else 0.000001)
                    
                    for r in readable:
                        if r is peer_socket:
                            data, addr = r.recvfrom(1024)
                            res = json.loads(data.decode('utf-8', 'ignore'))
                            command = res['command']
                            if command == 'GOSSIP':
                                self.on_gossiped(int(peer_socket.getsockname()[1]), r, res)
                            elif command == 'GOSSIP_REPLY':
                                self.on_gossip_replied(res)
                        elif r is client_socket:
                            conn, addr = r.accept()
                            conn.setblocking(False)
                            clients.append(conn)
                        elif r in clients:
                            data = r.recv(1024)
                            if not data or len(data) == 0:
                                clients.remove(r)
                            else:
                                req = json.loads(data.decode('utf-8', 'ignore'))
                                command = req['command']
                                if command == 'SET':
                                    i = req['index']
                                    if i >= 0 and i < len(self.words):
                                        self.words[i] = req['value']
                                        reply = {
                                            'command': 'SET_REPLY',
                                            'words': self.words
                                        }
                                        r.sendall(json.dumps(reply).encode())
                        else:
                            pass
                    
                    self.clear_expired_peers()
                    
                    if event.name == 'gossip':
                        n = min(5, len(self.peers))
                        peers = random.sample(list(self.peers.values()), n)
                        for p in peers:
                            gossip = {
                                "command": 'GOSSIP',
                                "host": self.host,
                                "port": self.peer_port,
                                "name": 'Me',
                                "messageID": str(uuid.uuid4())
                            }
                            peer_socket.sendto(json.dumps(gossip).encode(), (p.host, p.port))
                        self.events[event.id].renew(time.time() + 60)
                    elif event.name == 'consensus':
                        pass
                    
                except socket.timeout:
                    pass
                except KeyboardInterrupt:
                    sys.exit(0)
                except Exception as e:
                    print(e)

    def generate_peer_key(self, res):
        return f"{res['host']}:{res['port']}"

    def is_self(self, key):
        return key == f"{self.host}:{self.peer_port}"
    
    def log_peers(self):
        print('------ Current Peers --------')
        for k, v in self.peers.items():
            print(k, v.expiry)
        print('-----------------------------')
        
    def log_gossips_received(self):
        print('------ Gossip IDs Received --------')
        for id in self.gossips_received:
            print(id)
        print('-----------------------------------')

    def on_gossip_replied(self, res):
        key = self.generate_peer_key(res)
        
        if key not in self.peers and not self.is_self(key):
            peer = self.construct_peer(res)
            self.peers[key] = peer
        else:
            self.peers[key].renew()

    def on_gossiped(self, port, peer_socket, res):
        gossip_id = res['messageID']
        if gossip_id not in self.gossips_received:
            self.gossips_received.append(gossip_id)

            key = self.generate_peer_key(res)
            if key not in self.peers and not self.is_self(key):
                peer = self.construct_peer(res)
                self.peers[key] = peer
                
                reply = {
                    'command': 'GOSSIP_REPLY',
                    'host': socket.gethostname(),
                    'port': port,
                    'name': 'Me reply',
                }                     
                peer_socket.sendto(json.dumps(reply).encode(), (peer.host, peer.port))
                
            else:
                self.peers[key].renew()
        else:
            pass
            
def main():
    specified_port = int(sys.argv[1]) if len(sys.argv) > 1 else None
    Server(specified_port).start()

if __name__ == "__main__":
    main()
