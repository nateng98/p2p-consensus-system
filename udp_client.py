import socket
import time
import uuid
import json

clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
clientSocket.bind(('', 0))

ip = socket.gethostbyname(socket.gethostname())
port = clientSocket.getsockname()[1]

def gossip(clientSocket):
    targetHost = 'xiaoranmeng'
    targetPort = int(input("Port: "))
    gossip = {
        "command": 'GOSSIP',
        "host": ip,
        "port": port,
        "name": 'Me',
        "messageID": str(uuid.uuid4())
    }
    clientSocket.sendto(json.dumps(gossip).encode(), (targetHost, targetPort))
    data, address = clientSocket.recvfrom(1024)
    response = json.loads(data.decode('utf-8', 'ignore'))
    print(response)
    
    
    
while True:
    try:
        gossip(clientSocket)
        time.sleep(1)
    except KeyboardInterrupt:
        break

clientSocket.close()


