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
        self.counter = 0
        self.gossipsReceived = []
        self.peers = {}
        self.addWellKnownHosts()
        self.words = ['', '', '', '', '']
        self.events = {}
        self.consensus_results = {}
        self.lie_mode = False
        e = Event(str(uuid.uuid4()), 'gossip', time.time() + 60)
        self.events[e.id] = e
        
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

    def processCLICommand(self, command, conn):
        if command == 'peers':
            response = {
                'command': 'peers',
                'peers': {key: peer.name for key, peer in self.peers.items()}
            }
        elif command == 'current':
            response = {
                'command': 'current',
                'words': self.words
            }
        elif command.startswith('consensus '):
            index = int(command.split()[1])
            response = self.runConsensus(index)
        elif command == 'lie':
            self.lie_mode = True
            response = {'command': 'lie', 'status': 'started'}
        elif command == 'truth':
            self.lie_mode = False
            response = {'command': 'truth', 'status': 'stopped'}
        elif command.startswith('set '):
            _, index, word = command.split()
            self.words[int(index)] = word
            response = {'command': 'set', 'status': 'done'}
        elif command == 'exit':
            response = {'command': 'exit', 'status': 'closing'}
            conn.sendall(json.dumps(response).encode())
            conn.close()
            sys.exit(0)
        else:
            response = {'command': 'error', 'message': 'Unknown command'}
        
        conn.sendall(json.dumps(response).encode())

    def runConsensus(self, index):
        message_id = str(uuid.uuid4())
        value = self.words[index]
        peers = [f'{p.host}:{p.port}' for p in self.peers.values()]
        due = int(time.time()) + 10

        consensus_message = {
            'command': 'CONSENSUS',
            'OM': len(peers) - 1,
            'index': index,
            'value': value,
            'peers': peers,
            'messageID': message_id,
            'due': due
        }
        self.broadcastConsensus(consensus_message)

        return {'command': 'consensus', 'status': 'started'}

    def broadcastConsensus(self, message):
        peerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for peer in self.peers.values():
            peerSocket.sendto(json.dumps(message).encode(), (peer.host, peer.port))
        peerSocket.close()

    def handleConsensusReply(self, res):
        message_id = res['reply-to']
        if message_id not in self.consensus_results:
            self.consensus_results[message_id] = []

        self.consensus_results[message_id].append(res['value'])

        if len(self.consensus_results[message_id]) >= len(self.peers):
            final_value = max(set(self.consensus_results[message_id]), key=self.consensus_results[message_id].count)
            print(f"Consensus reached: {final_value}")
            self.consensus_results[message_id] = final_value

    def start(self):
        clientSocket, peerSocket = self.createSockets()
        
        print(clientSocket)
        print(peerSocket)
        
        inputs = [clientSocket, peerSocket]
        clients = []
        outputs = []
        
        with clientSocket, peerSocket:
            while True:
                try:
                    event = self.nextEvent()
                    timeout = event.expiry - time.time()
                            
                    readable, writable, exceptional = select.select(
                        inputs + clients,
                        outputs,
                        inputs + clients,
                        timeout if timeout > 0 else 0.000001)
                    
                    print('Processing readable')
                    
                    for r in readable:
                        if r is peerSocket:
                            data, addr = r.recvfrom(1024)
                            res = json.loads(data.decode('utf-8', 'ignore'))
                            print(f"UDP message from {addr}: {res}")
                            command = res['command']
                            if command == 'GOSSIP':
                                print('Gossip received')
                                self.onGossiped(int(peerSocket.getsockname()[1]), r, res)
                            elif command == 'GOSSIP_REPLY':
                                print('Gossip replied')
                                self.onGossipReplied(res)
                            elif command == 'CONSENSUS':
                                print('Consensus received')
                                self.onConsensusReceived(peerSocket, res)
                            elif command == 'CONSENSUS-REPLY':
                                print('Consensus reply received')
                                self.handleConsensusReply(res)
                        elif r is clientSocket:
                            conn, addr = r.accept()
                            print('Connected from: ', addr)
                            conn.setblocking(False)
                            clients.append(conn)
                        elif r in clients:
                            data = r.recv(1024)
                            if not data or len(data) == 0:
                                clients.remove(r)
                            else:
                                command = data.decode('utf-8', 'ignore').strip()
                                print('Command:', command)
                                self.processCLICommand(command, r)
                        else:
                            print('Readable not found')
                    
                    self.clearExpiredPeers()
                    
                    if event.name == 'gossip':
                        print('Gossiping to peers')
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
                    elif event.name == 'consensus':
                        pass
                    
                except socket.timeout as e:
                    print('Time out')
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
            print(f'Peer {key} added')
        else:
            print(f'Renewing peer {key} expiry')
            self.peers[key].renew()

    def onGossiped(self, port, peerSocket, res):
        gossipId = res['messageID']
        if gossipId not in self.gossipsReceived:
            self.counter += 1
            print('Counter - ', self.counter)
            print(f'Gossip with ID {gossipId} is added')
            print(f'------------- Gossip content --------')
            print(res)
            print(f'-------------------------------------')
            self.gossipsReceived.append(gossipId)

            key = self.generatePeerKey(res)
            if key not in self.peers and not self.isSelf(key):
                peer = self.constructPeer(res)
                self.peers[key] = peer
                print(f'Peer {key} added')
                
                reply = {
                    'command': 'GOSSIP_REPLY',
                    'host': socket.gethostname(),
                    'port': port,
                    'name': 'Me reply',
                }                     
                peerSocket.sendto(json.dumps(reply).encode(), (peer.host, peer.port))
                
            else:
                print(f'Renewing peer {key} expiry')
                self.peers[key].renew()
        else:
            print(f'Gossip {gossipId} exists')

    def onConsensusReceived(self, peerSocket, res):
        om_level = res['OM']
        if om_level == 0:
            # OM(0): Return the current value
            reply = {
                'command': 'CONSENSUS-REPLY',
                'value': res['value'],
                'reply-to': res['messageID']
            }
            peerSocket.sendto(json.dumps(reply).encode(), (res['host'], res['port']))
        else:
            # OM(m): Perform a sub-consensus
            sub_message_id = str(uuid.uuid4())
            sub_peers = [peer for peer in res['peers'] if peer != f'{self.host}:{self.peerPort}']
            sub_due = res['due'] - 1  # Sub-consensus should reply earlier

            sub_consensus_message = {
                'command': 'CONSENSUS',
                'OM': om_level - 1,
                'index': res['index'],
                'value': res['value'],
                'peers': sub_peers,
                'messageID': sub_message_id,
                'due': sub_due
            }
            self.broadcastConsensus(sub_consensus_message)
            
            # Handle sub-consensus reply
            self.consensus_results[sub_message_id] = []
            start_time = time.time()
            while time.time() - start_time < sub_due:
                try:
                    data, addr = peerSocket.recvfrom(1024)
                    reply = json.loads(data.decode('utf-8', 'ignore'))
                    if reply['command'] == 'CONSENSUS-REPLY' and reply['reply-to'] == sub_message_id:
                        self.consensus_results[sub_message_id].append(reply['value'])
                        if len(self.consensus_results[sub_message_id]) >= len(sub_peers):
                            break
                except socket.timeout:
                    pass

            if self.consensus_results[sub_message_id]:
                final_value = max(set(self.consensus_results[sub_message_id]), key=self.consensus_results[sub_message_id].count)
            else:
                final_value = res['value']

            reply = {
                'command': 'CONSENSUS-REPLY',
                'value': final_value,
                'reply-to': res['messageID']
            }
            peerSocket.sendto(json.dumps(reply).encode(), (res['host'], res['port']))
            
def main():
    specified_port = int(sys.argv[1]) if len(sys.argv) > 1 else None
    Server(specified_port).start()

if __name__ == "__main__":
    main()
