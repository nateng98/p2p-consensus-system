import socket
import json
import random
import threading
import time

# Constants
GOSSIP_INTERVAL = 60  # seconds
GOSSIP_TIMEOUT = 120  # seconds
NUM_PEERS_TO_GOSSIP = 5
DATABASE_SIZE = 5

class Peer:
    def __init__(self, host, port, name):
        self.host = host
        self.port = port
        self.name = name
        self.last_gossip_time = time.time()

    def to_dict(self):
        return {
            "host": self.host,
            "port": self.port,
            "name": self.name
        }

class Node:
    def __init__(self, port):
        self.port = port
        self.peers = []
        self.database = [""] * DATABASE_SIZE
        self.active = True
        self.lie = False
        self.lie_percentage = 0

        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.bind(('localhost', self.port))
        self.tcp_socket.listen()

        # Start gossiping thread
        self.gossip_thread = threading.Thread(target=self.gossip)
        self.gossip_thread.daemon = True
        self.gossip_thread.start()

    def handle_udp_message(self, data, addr):
        try:
            message = json.loads(data.decode())
            if message['command'] == 'GOSSIP':
                self.handle_gossip(message, addr)
            elif message['command'] == 'QUERY':
                self.handle_query(addr)
        except json.JSONDecodeError:
            pass

    def handle_gossip(self, message, addr):
        peer = Peer(message['host'], message['port'], message['name'])
        if peer not in self.peers:
            self.peers.append(peer)
            self.send_gossip_reply(peer, addr)
        else:
            for p in self.peers:
                if p.host == message['host'] and p.port == message['port']:
                    p.last_gossip_time = time.time()

    def send_gossip_reply(self, peer, addr):
        reply_message = {
            "command": "GOSSIP_REPLY",
            "host": "localhost",
            "port": self.port,
            "name": "Your Node Name"
        }
        self.udp_socket.sendto(json.dumps(reply_message).encode(), addr)

    def handle_query(self, addr):
        reply_message = {
            "command": "QUERY-REPLY",
            "database": self.database
        }
        self.udp_socket.sendto(json.dumps(reply_message).encode(), addr)

    def gossip(self):
        while self.active:
            # GOSSIP
            for _ in range(min(NUM_PEERS_TO_GOSSIP, len(self.peers))):
                peer = random.choice(self.peers)
                gossip_message = {
                    "command": "GOSSIP",
                    "host": "localhost",
                    "port": self.port,
                    "name": "Your Node Name"
                }
                self.udp_socket.sendto(json.dumps(gossip_message).encode(),
                                        (peer.host, peer.port))

            # Check for inactive peers
            current_time = time.time()
            self.peers = [p for p in self.peers if current_time - p.last_gossip_time <= GOSSIP_TIMEOUT]

            time.sleep(GOSSIP_INTERVAL)

    def start_cli(self):
        while self.active:
            conn, _ = self.tcp_socket.accept()
            threading.Thread(target=self.handle_cli_connection, args=(conn,)).start()

    def handle_cli_connection(self, conn):
        try:
            conn.sendall(b"Welcome to the Node CLI!\n")
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                command = data.decode().strip()
                response = self.execute_command(command)
                conn.sendall(response.encode())
        finally:
            conn.close()

    def execute_command(self, command):
        if command == "peers":
            return json.dumps([peer.to_dict() for peer in self.peers])
        elif command == "current":
            return json.dumps(self.database)
        # Implement other commands
        else:
            return "Invalid command."

    def stop(self):
        self.active = False
        self.tcp_socket.close()
        self.udp_socket.close()

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    node = Node(port)
    threading.Thread(target=node.start_cli).start()
