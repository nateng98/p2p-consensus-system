import sys
import select
import json
import socket
import time
import random
import uuid
from collections import defaultdict

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

class Server:
    def __init__(self, specified_port=None, debug=False):
        self.peers = {}
        self.addWellKnownHosts()
        self.words = ['', '', '', '', '']
        self.events = {}
        e = Event(str(uuid.uuid4()), 'gossip', time.time() + 60)
        self.events[e.id] = e
        self.is_lying = False
        self.lie_probability = 0.0  # Probability of lying, between 0 and 1
        self.client_sockets = []
        self.debug = debug
        self.pending_consensus = defaultdict(list)

        self.host = None
        self.clientPort = 15000 if specified_port == 16000 else None
        self.peerPort = 16000 if specified_port == 16000 else None
        self.gossipsReceived = []

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
        return clientSocket, peerSocket
    
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
        return min(self.events.values(), key=lambda e: e.expiry)
    
    def clearExpiredPeers(self):
        keys = [key for key, value in self.peers.items() if value.expiry < time.time()]
        for key in keys:
            del self.peers[key]
            if self.debug:
                print(f'Cleared peer {key}')

    def start(self):
        clientSocket, peerSocket = self.createSockets()
        self.peerSocket = peerSocket  # Store peerSocket as an instance variable
        inputs = [clientSocket, peerSocket]
        
        with clientSocket, peerSocket:
            while True:
                try:
                    event = self.nextEvent()
                    timeout = event.expiry - time.time()
                            
                    readable, _, exceptional = select.select(
                        inputs + self.client_sockets,
                        [],
                        inputs + self.client_sockets,
                        timeout if timeout > 0 else 0.000001)
                    
                    for r in readable:
                        if r is peerSocket:
                            self.handlePeerSocket(r)
                        elif r is clientSocket:
                            self.handleClientSocket(r)
                        elif r in self.client_sockets:
                            self.handleClientData(r)
                    
                    self.clearExpiredPeers()
                    self.handleEvent(event)
                    
                except socket.timeout:
                    continue
                except KeyboardInterrupt:
                    sys.exit(0)
                except Exception as e:
                    if self.debug:
                        print(e)

    def handlePeerSocket(self, sock):
        data, addr = sock.recvfrom(1024)
        res = json.loads(data.decode('utf-8', 'ignore'))
        command = res['command']
        if command == 'GOSSIP':
            self.onGossiped(int(sock.getsockname()[1]), sock, res)
        elif command == 'GOSSIP_REPLY':
            self.onGossipReplied(res)
        elif command == 'CONSENSUS':
            self.onConsensusReceived(sock, res)
        elif command == 'CONSENSUS-REPLY':
            self.onConsensusReplyReceived(res)

    def handleClientSocket(self, sock):
        conn, addr = sock.accept()
        conn.setblocking(False)
        self.client_sockets.append(conn)

    def handleClientData(self, conn):
        data = conn.recv(1024)
        if not data:
            self.client_sockets.remove(conn)
        else:
            self.handleCLI(conn, data)
    
    def handleEvent(self, event):
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
                self.peerSocket.sendto(json.dumps(gossip).encode(), (p.host, p.port))
            self.events[event.id].renew(time.time() + 60)
    
    def generatePeerKey(self, res):
        return res['host'] + ':' + str(res['port'])

    def isSelf(self, key):
        return key == self.host + ':' + str(self.peerPort)

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
    
    def onConsensusReceived(self, sock, res):
        OM_level = res['OM']
        index = res['index']
        value = res['value']
        peers = res['peers']
        messageID = res['messageID']
        due = res['due']

        print(f"CONSENSUS received with message ID {messageID} at OM level {OM_level}")

        if OM_level > 0:
            sub_consensus_value = self.initiateConsensus(index, OM_level - 1, value, peers, messageID, due)
            if self.is_lying and random.random() < self.lie_probability:
                sub_consensus_value = 'LIE'  # Lying about the consensus value
            reply = {
                'command': 'CONSENSUS-REPLY',
                'value': sub_consensus_value,
                'reply-to': messageID
            }
            print(f"Sending CONSENSUS-REPLY for message ID {messageID} with value {sub_consensus_value}")
            sock.sendto(json.dumps(reply).encode(), (res['host'], res['port']))
        else:
            if self.is_lying and random.random() < self.lie_probability:
                value = 'LIE'  # Lying about the consensus value
            self.words[index] = value
            reply = {
                'command': 'CONSENSUS-REPLY',
                'value': value,
                'reply-to': messageID
            }
            print(f"Sending CONSENSUS-REPLY for message ID {messageID} with value {value}")
            sock.sendto(json.dumps(reply).encode(), (res['host'], res['port']))

    def onConsensusReplyReceived(self, res):
        messageID = res['reply-to']
        value = res['value']
        
        # Store the received value
        self.pending_consensus[messageID].append(value)
        
        # Check if all responses are received
        if len(self.pending_consensus[messageID]) == len(self.peers) - 1:
            consensus_value = max(set(self.pending_consensus[messageID]), key=self.pending_consensus[messageID].count)
            print(f"Consensus for message ID {messageID} is {consensus_value}")
            
            # Optionally, update the word list if this node was the initiator
            # You need to track if this node initiated the consensus to do the update
            # self.words[index] = consensus_value
            
            # Clean up the stored responses
            del self.pending_consensus[messageID]
    
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
        print(f"Sending UDP command to {host}:{port} with command {command}")
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
            if len(command) > 1:
                self.lie_probability = float(command[1])
            else:
                self.lie_probability = 0.5  # Default probability of lying
            conn.sendall(f"Node will now lie with probability {self.lie_probability}.\n".encode())
        elif command[0] == 'truth':
            self.is_lying = False
            self.lie_probability = 0.0
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
    specified_port = None
    debug = False

    for arg in sys.argv[1:]:
        if arg == '--debug':
            debug = True
        else:
            specified_port = int(arg)

    Server(specified_port, debug).start()

if __name__ == "__main__":
    main()
