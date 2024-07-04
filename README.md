# Peer-to-Peer - Distributed Consensus Sytem

## *Note: Well-known peers are down after June 14, 2024 but this is still a good example of p2p consensus system written in Python

### How to start a Peer

- Start a server/peer/node

```
python3 server.py
```
or
```
python3 server.py 16000
```
The program will print on console
```
Client | TCP | Host 130.179.28.114 | falcon.cs.umanitoba.ca | Port 39459
Peer   | UDP | Host 130.179.28.114 | falcon.cs.umanitoba.ca | Port 39734
```

### How to connect as CLI
- Client can use telnet with given Host and (Client's) Port 

```
telnet 130.179.28.114 39459
```
or
```
telnet falcon.cs.umanitoba.ca 39459
```

### CLIs' commands
- `peers` - list all known peers.
- `current` - the current word list.
- `consensus <x>` - where x is a numeric index. Run a consensus for this word index.
- `lie` or `lie <x>` - begin lying. Lie either all the time (default) or with x where is a probability value from 0 to 1.
- `truth` - stop lying. Always return 'honest' answers.
- `set <x> <y>` - where x is an index, and y is a word. Set the value x to word y.
- `exit` - close the command-line interface.
