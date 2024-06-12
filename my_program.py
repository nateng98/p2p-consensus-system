import sys
import select
import json
import socket
import time
import random
import uuid
import re
import logging

# Set up logging for debugging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class Node:
    def __init__(self, host, port, known_peers):
        self.host = host
        self.port = port
        self.known_peers = known_peers
        self.peers = {}
        self.database = [""] * 5
        self.lie = False
        self.lie_percentage = 0
        self.cli_socket = None
        self.server_socket = None
        self.setup_sockets()

    def setup_sockets(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        logging.info(f"Node started on {self.host}:{self.port}")

    def start(self):
        self.announce()
        while True:
            readable, _, _ = select.select([self.server_socket] + list(self.peers.values()), [], [], 60)
            for s in readable:
                if s is self.server_socket:
                    client_socket, addr = s.accept()
                    logging.info(f"Connection from {addr}")
                    self.peers[addr] = client_socket
                else:
                    self.handle_client_message(s)
            self.gossip()

    def announce(self):
        gossip_message = json.dumps({
            "command": "GOSSIP",
            "host": self.host,
            "port": self.port,
            "name": socket.gethostname(),
            "messageID": str(uuid.uuid4())
        })
        for peer in self.known_peers:
            self.send_message(peer, gossip_message)

    def gossip(self):
        gossip_message = json.dumps({
            "command": "GOSSIP",
            "host": self.host,
            "port": self.port,
            "name": socket.gethostname(),
            "messageID": str(uuid.uuid4())
        })
        peers_to_notify = random.sample(list(self.peers.keys()), min(5, len(self.peers)))
        for peer in peers_to_notify:
            self.send_message(peer, gossip_message)

    def handle_client_message(self, client_socket):
        try:
            message = client_socket.recv(4096)
            if message:
                data = json.loads(message.decode('utf-8'))
                logging.info(f"Received message: {data}")
                self.process_message(data, client_socket)
            else:
                self.remove_peer(client_socket)
        except Exception as e:
            logging.error(f"Error handling message: {e}")
            self.remove_peer(client_socket)

    def process_message(self, message, client_socket):
        command = message.get("command")
        if command == "GOSSIP":
            self.handle_gossip(message, client_socket)
        elif command == "SET":
            self.handle_set(message)
        elif command == "CONSENSUS":
            self.handle_consensus(message, client_socket)
        elif command == "QUERY":
            self.handle_query(client_socket)

    def handle_gossip(self, message, client_socket):
        peer_info = (message["host"], message["port"])
        if peer_info not in self.peers:
            self.peers[peer_info] = client_socket
            logging.info(f"New peer added: {peer_info}")
        reply_message = json.dumps({
            "command": "GOSSIP_REPLY",
            "host": self.host,
            "port": self.port,
            "name": socket.gethostname()
        })
        self.send_message(peer_info, reply_message)
        self.gossip()

    def handle_set(self, message):
        index = message.get("index")
        value = message.get("value")
        if 0 <= index < len(self.database):
            self.database[index] = value
            logging.info(f"Database updated at index {index} with value '{value}'")

    def handle_consensus(self, message, client_socket):
        # Simplified consensus logic
        index = message.get("index")
        if 0 <= index < len(self.database):
            consensus_value = self.database[index]
            if self.lie and random.random() < self.lie_percentage:
                consensus_value = "incorrect_value"
            reply_message = json.dumps({
                "command": "CONSENSUS-REPLY",
                "value": consensus_value,
                "reply-to": message["messageID"]
            })
            client_socket.send(reply_message.encode('utf-8'))
            logging.info(f"Consensus reply sent: {reply_message}")

    def handle_query(self, client_socket):
        reply_message = json.dumps({
            "command": "QUERY-REPLY",
            "database": self.database
        })
        client_socket.send(reply_message.encode('utf-8'))
        logging.info(f"Query reply sent: {reply_message}")

    def send_message(self, peer_info, message):
        try:
            host, port = peer_info
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
                s.sendall(message.encode('utf-8'))
        except Exception as e:
            logging.error(f"Failed to send message to {peer_info}: {e}")

    def remove_peer(self, client_socket):
        peer_info = None
        for key, value in self.peers.items():
            if value is client_socket:
                peer_info = key
                break
        if peer_info:
            del self.peers[peer_info]
            logging.info(f"Peer removed: {peer_info}")
        client_socket.close()

class CLI:
    def __init__(self, node):
        self.node = node
        self.cli_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.cli_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.cli_socket.bind((self.node.host, self.node.port + 1000))  # CLI on a different port
        self.cli_socket.listen(5)
        logging.info(f"CLI started on {self.node.host}:{self.node.port + 1000}")

    def start(self):
        while True:
            client_socket, addr = self.cli_socket.accept()
            logging.info(f"CLI connection from {addr}")
            self.handle_client(client_socket)

    def handle_client(self, client_socket):
        with client_socket:
            while True:
                client_socket.send(b"> ")
                command = client_socket.recv(4096).decode('utf-8').strip()
                if command:
                    logging.info(f"CLI command received: {command}")
                    response = self.execute_command(command)
                    client_socket.sendall(response.encode('utf-8'))

    def execute_command(self, command):
        parts = command.split()
        if parts[0] == "peers":
            return json.dumps(self.node.peers)
        elif parts[0] == "current":
            return json.dumps(self.node.database)
        elif parts[0] == "consensus":
            if len(parts) == 2 and parts[1].isdigit():
                self.node.handle_consensus({"command": "CONSENSUS", "index": int(parts[1]), "messageID": str(uuid.uuid4())}, None)
                return "Consensus started"
            else:
                return "Invalid command"
        elif parts[0] == "lie":
            self.node.lie = True
            self.node.lie_percentage = float(parts[1]) if len(parts) > 1 else 1.0
            return f"Lying with {self.node.lie_percentage * 100}% probability"
        elif parts[0] == "truth":
            self.node.lie = False
            return "Truth mode activated"
        elif parts[0] == "set":
            if len(parts) == 3 and parts[1].isdigit():
                index = int(parts[1])
                value = parts[2]
                self.node.handle_set({"index": index, "value": value})
                return "Value set"
            else:
                return "Invalid command"
        elif parts[0] == "exit":
            return "Goodbye"
        else:
            return "Unknown command"

# Running the node
if __name__ == "__main__":
    known_peers = [("owl.cs.umanitoba.ca", 16000)]
    port = int(sys.argv[1]) if len(sys.argv) > 1 else random.randint(10000, 20000)
    node = Node("0.0.0.0", port, known_peers)
    cli = CLI(node)
    try:
        node.start()
        cli.start()
    except KeyboardInterrupt:
        logging.info("Shutting down node")
        for sock in node.peers.values():
            sock.close()
        node.server_socket.close()
        cli.cli_socket.close()
