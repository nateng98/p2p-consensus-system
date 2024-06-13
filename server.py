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
        print(self.host, self.port, self.name)

class Server:
    
    def __init__(self, specified_port=None):
        self.counter = 0 # Debug counter for gossip IDs appended
        self.gossipsReceived = []
        self.peers = {}
        self.addWellKnownHosts()
        self.words = ['', '', '', '', '']
        self.events = {}
        e = Event(str(uuid.uuid4()), 'gossip', time.time() + 60)
        self.events[e.id] = e
        self.is_lying = False
        self.client_sockets = []

        # Create a class for this
        self.host = None
        self.clientPort = 15000 if specified_port == 16000 else None
        self.peerPort = 16000 if specified_port == 16000 else None

    def addWellKnownHosts(self):
        for host in WELL_KNOWN_HOSTS:
            key = host + ':16000'
            self.peers[key] = Peer(host, 16000, 'WK')
    
    def createSockets(self):
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.configSocket(clientSocket, self.clientPort)
        self.configSocket(peerSocket, self.peerPort)
        clientSocket.listen(5)
        self.setServerInfo(clientSocket, peerSocket)
        self.logSocketsInfo()
        return (clientSocket, peerSocket)
    
    def setServerInfo(self, clientSocket, peerSocket):
        self.host = socket.gethostname()
        self.clientPort = clientSocket.getsockname()[1]
        self.peerPort = peerSocket.getsockname()[1]
    
    def logSocketsInfo(self):
        print(f"Client | TCP | Port {self.clientPort} | Host {self.host}")
        print(f"Peer   | UDP | Port {self.peerPort} | Host {self.host}")
         
    def configSocket(self, s, port=None):
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setblocking(False)
        if port:
            s.bind(('', port))
        else:
            s.bind(('', 0))
        
    def constructPeer(self, gossip):
        return Peer(gossip['host'], gossip['port'], gossip['name'])
    
    def nextEvent(self):
        return min(self.events.values(), key = lambda e: e.expiry)
    
    def clearExpiredPeers(self):
        keys = []
        for key, value in self.peers.items():
            if value.expiry < int(time.time()):
                keys.append(key)
        for key in keys:
            del self.peers[key]
            print(f'Cleared peer {key}')

    def start(self):
        clientSocket, peerSocket = self.createSockets()
        inputs = [clientSocket, peerSocket]
        
        with clientSocket, peerSocket:
            while True:
                try:
                    event = self.nextEvent()
                    timeout = event.expiry - time.time()
                            
                    readable, writable, exceptional = select.select(
                        inputs + self.client_sockets,
                        [],
                        inputs + self.client_sockets,
                        timeout if timeout > 0 else 0.000001)
                    
                    for r in readable:
                        if r is peerSocket:
                            data, addr = r.recvfrom(1024)
                            res = json.loads(data.decode('utf-8', 'ignore'))
                            command = res['command']
                            if command == 'GOSSIP':
                                self.onGossiped(int(peerSocket.getsockname()[1]), r, res)
                            elif command == 'GOSSIP_REPLY':
                                self.onGossipReplied(res)
                            elif command == 'CONSENSUS':
                                self.onConsensusReceived(r, res)
                        elif r is clientSocket:
                            conn, addr = r.accept()
                            conn.setblocking(False)
                            self.client_sockets.append(conn)
                        elif r in self.client_sockets:
                            data = r.recv(1024)
                            if not data:
                                self.client_sockets.remove(r)
                            else:
                                self.handleCLI(r, data)
                    
                    self.clearExpiredPeers()
                    
                    if event.name == 'gossip':
                        n = min(5, len(self.peers))
                        peers = random.sample(list(self.peers.values()), n)
                        for p in peers:
                            gossip = {
                                "command": 'GOSSIP',
                                "host": self.host,
                                "port": self.peerPort,
                                "name": 'Me',
                                "messageID": str(uuid.uuid4())
                            }
                            peerSocket.sendto(json.dumps(gossip).encode(), (p.host, p.port))
                        self.events[event.id].renew(time.time() + 60)
                    
                except socket.timeout as e:
                    continue
                except KeyboardInterrupt as e:
                    sys.exit(0)
                except Exception as e:
                    print(e)

    def generatePeerKey(self, res):
        return res['host'] + ':' + str(res['port'])

    def isSelf(self, key):
        return key == self.host + ':' + str(self.peerPort)
    
    def logPeers(self):
        print('------ Current Peers --------')
        for k, v in self.peers.items():
            print(k, v.expiry)
        print('-----------------------------')
        
    def logGossipsReceived(self):
        print('------ Gossip IDs Received --------')
        for id in self.gossipsReceived:
            print(id)
        print('-----------------------------------')

    def onGossipReplied(self, res):
        key = self.generatePeerKey(res)
        
        if key not in self.peers and not self.isSelf(key):
            peer = self.constructPeer(res)
            self.peers[key] = peer
        else:
            self.peers[key].renew()

    def onGossiped(self, port, peerSocket, res):
        gossipId = res['messageID']
        if gossipId not in self.gossipsReceived:
            self.counter += 1
            self.gossipsReceived.append(gossipId)

            key = self.generatePeerKey(res)
            if key not in self.peers and not self.isSelf(key):
                peer = self.constructPeer(res)
                self.peers[key] = peer
                
                reply = {
                    'command': 'GOSSIP_REPLY',
                    'host': socket.gethostname(),
                    'port': port,
                    'name': 'Me reply',
                }                     
                peerSocket.sendto(json.dumps(reply).encode(), (peer.host, peer.port))
                
            else:
                self.peers[key].renew()
        else:
            print(f'Gossip {gossipId} exists')
    
    def onConsensusReceived(self, sock, res):
        OM_level = res['OM']
        index = res['index']
        value = res['value']
        peers = res['peers']
        messageID = res['messageID']
        due = res['due']

        if OM_level > 0:
            sub_consensus_value = self.initiateConsensus(index, OM_level - 1, value, peers, messageID, due)
            reply = {
                'command': 'CONSENSUS-REPLY',
                'value': sub_consensus_value,
                'reply-to': messageID
            }
            sock.sendto(json.dumps(reply).encode(), (res['host'], res['port']))
        else:
            self.words[index] = value
            reply = {
                'command': 'CONSENSUS-REPLY',
                'value': value,
                'reply-to': messageID
            }
            sock.sendto(json.dumps(reply).encode(), (res['host'], res['port']))
    
    def initiateConsensus(self, index, OM_level=0, value=None, peers=None, messageID=None, due=None):
        if peers is None:
            peers = [peer for peer in self.peers.keys() if not self.isSelf(peer)]
        if messageID is None:
            messageID = str(uuid.uuid4())
        if value is None:
            value = self.words[index]
        if due is None:
            due = int(time.time()) + 30

        command = {
            'command': 'CONSENSUS',
            'OM': OM_level,
            'index': index,
            'value': value,
            'peers': peers,
            'messageID': messageID,
            'due': due
        }

        for peer in peers:
            peer_host, peer_port = peer.split(':')
            self.sendUDPCommand(command, peer_host, int(peer_port))

        responses = []
        start_time = time.time()
        while time.time() - start_time < 30:
            try:
                data, addr = self.peerSocket.recvfrom(1024)
                res = json.loads(data.decode('utf-8', 'ignore'))
                if res['reply-to'] == messageID:
                    responses.append(res['value'])
            except socket.timeout:
                continue
        
        consensus_value = max(set(responses), key=responses.count)
        self.words[index] = consensus_value
        return consensus_value

    def sendUDPCommand(self, command, host, port):
        self.peerSocket.sendto(json.dumps(command).encode(), (host, port))
        
    def handleCLI(self, conn, data):
        command = data.decode().strip().split(' ')
        if command[0] == 'peers':
            peers_info = {k: v.__dict__ for k, v in self.peers.items()}
            conn.sendall((json.dumps(peers_info, indent=2) + '\n').encode())
        elif command[0] == 'current':
            conn.sendall((json.dumps(self.words, indent=2) + '\n').encode())
        elif command[0] == 'consensus' and len(command) > 1:
            index = int(command[1])
            self.initiateConsensus(index)
            conn.sendall((f"Consensus on index {index} completed.\n").encode())
        elif command[0] == 'lie':
            self.is_lying = True
            conn.sendall("Node will now lie.\n".encode())
        elif command[0] == 'truth':
            self.is_lying = False
            conn.sendall("Node will now tell the truth.\n".encode())
        elif command[0] == 'set' and len(command) > 2:
            index = int(command[1])
            word = command[2]
            self.words[index] = word
            conn.sendall((f"Word at index {index} set to {word}.\n").encode())
        elif command[0] == 'exit':
            conn.sendall("Exiting CLI.\n".encode())
            self.client_sockets.remove(conn)
            conn.close()
        else:
            conn.sendall("Invalid command.\n".encode())

def main():
    specified_port = int(sys.argv[1]) if len(sys.argv) > 1 else None
    Server(specified_port).start()

if __name__ == "__main__":
    main()