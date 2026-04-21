
# Replicated Distributed File System (DFS)

## Overview
This project is a Distributed File System (DFS) built on a Chord-based Distributed Hash Table with strong consistency provided by a simplified Paxos consensus protocol. It supports **Distributed file stroage** using Chord lookups, **Page-based file metadata management**, and **Distributed sorting of file contents**. Chord assigns each key to the node responsible for its successor on the identifier ring, and uses finger tables to achieve logarithmic routing. Replication improves reliability and performance, but introduces consistency challenges. Paxos addresses the fault-tolerance side by ensuring that replicas execute operations in the same order despite crashes and lost or delayed messages.

## System Architecture
```text
+------------------------------------------+
|            DFS Client (CLI)              |
|        (dfs_layer.py test script)        |
+------------------------------------------+
                    |
                    v
+------------------------------------------+
|               DFS API Layer              |
|          (DFS Class in dfs_layer.py)     |
|  - touch, append, read, delete           |
|  - sort_file (Distributed Sorting Logic) |
+------------------------------------------+
                    |
                    v
+------------------------------------------+
|           Paxos Consensus Layer          |
|      (Part of chord_layer.py proxy)      |
|  - Sequence (t) Generation               |
|  - ACCEPT/LEARN Majority-based commitment|
+------------------------------------------+
                    |
                    v
+------------------------------------------+
|           Chord Routing Layer            |
|       (NetworkChordRing in chord_layer)  |
|  - locate_successor (NodeID mapping)     |
|  - Order-Preserving Key Projection       |
+------------------------------------------+
                    |
                    v (Network/XML-RPC)
+------------------------------------------+
|           Distributed Storage            |
|        (node_server.py instances)         |
|  - Local DHT Storage (Metadata & Pages)  |
|  - Local Sort Buffers                    |
|  - Human-Readable Paxos Logs             |
+------------------------------------------+
```

## Required Protocol Concepts and Functional Requirements
- a **leader**
- **proposal/sequence number** or ballot number
- **ACCEPT** messages
- **LEARN** messages
- majority-based commitment with at least 3 replicas

For each replicated DFS update:
1. Leader proposes operation ```o```
3. Replicas receive ```ACCEPT(o, t)```
4. Replicas respond with ```LEARN(o, t)```
5. Operation is committed once a majority has learned it
6. All replicas apply committed operations in the same order

### Required Behavior
A distributed file is represented as:
- **Metadata object**
    - logical filename
    - total number of pages
    - file size
    - ordered list of page descriptors
- **Page objects**
    - each page stores a chunk of the file content
    - each page is stored in the Chord ring using a hash-derived key
 
### Required Distributed Sorting Model
1. Read each page of the input file
2. Parse each record ```(key, value)```
3. Route each record to the peer responsible for the successor of ```hash(key)``` or ```key``` itself, depending on your chosen design
4. At each responsible peer, insert incoming records into a local ordered structure
    - e.g. sorted list, balanced BST abstraction, heap followed by final sort, etc.
5. Produce a globally sorted output file
6. Store the sorted output as a new distributed file in the DFS

The final ```output_filename``` must contain all records from ```filename``` in globally sorted order.

---
## Requirements and Core Modules Used
- Python 3.8 or higher
- This project relies entirely on Python's built in modules. No external dependencies or ```pip install``` commands are required
- This system requires access to the local loopback interface (```localhost```) for IPC via RPC
- ```xmlrpc.server``` and ```xmlrpc.client``` are used for distributed network communication between nodes
- ```threading``` to handle concurrent requests on each peer node
- ```hashlib``` for SHA-1 deterministic hashing and Chord ring mapping
- ```json``` for file metadata serialization
- ```os``` and ```time``` for local file handling and Paxos sequence number generation
  
## Setup and Execution Instructions

### 1. Start the Peer Nodes
Open **5 separate terminal windows** and run one of these commands in each. Leave these windows open while testing.

**Terminal 1:**
```bash
python node_server.py 0 8000
```

**Terminal 2:**
```bash
python node_server.py 292300327466180583640736966543256603931186508595 8001
```

**Terminal 3:**
```bash
python node_server.py 584600654932361167281473933086513207862373017190 8002
```

**Terminal 4:**
```bash
python node_server.py 876900982398541750922210899629769811793559525785 8003
```

**Terminal 5:**
```bash
python node_server.py 1169201309864722334562947866173026415724746034380 8004
```

### 2. Run the DFS Client
Once the server terminals show they are listening, open a **6th terminal window** and run the main application logic:

**Terminal 6:**
```bash
python dfs_layer.py
```

## Project Structure

node_server.py  
→ Handles:
- storage (key-value data)
- replication (leader + majority)
- Paxos-style ACCEPT / LEARN / APPLY
- local sorting buffers

chord_layer.py  
→ Handles:
- key hashing
- successor lookup
- replica group selection
- routing client requests to correct node

dfs_layer.py  
→ Handles:
- DFS file operations (touch, append, read, delete)
- metadata and page management
- distributed sorting logic
- test execution and validation output

---

## Technical Implementation Details

### Chord Routing
- Uses deterministic successor lookup on a sorted node list
- No finger tables are implemented (simplified Chord model)
- All nodes are aware of full ring membership
- Ensures consistent and predictable key placement

---

### Replication Model
- Each key is replicated across 3 nodes:
  - successor (leader)
  - next 2 nodes in ring
- Leader = first node in replica group
- Writes require majority quorum (2 out of 3)
- Slot IDs are **string-based (time_ns + node_id)** to:
  - guarantee uniqueness
  - avoid XML-RPC integer overflow

---

### Paxos-Inspired Protocol
- Simplified implementation (no prepare phase)
- Uses:
  - ballot numbers
  - ACCEPT phase
  - LEARN phase
- Flow:
  1. Leader sends ACCEPT
  2. Majority responds
  3. Leader sends LEARN
  4. Replicas APPLY operation

---

### Metadata Management
- Stored as JSON objects
- Tracks:
  - file structure
  - pages
  - replica locations
- Version increments on updates

---

### Sorting Implementation
- Uses order-preserving mapping (not SHA-1)
- Ensures lexicographic ordering of keys
- Nodes locally sort assigned records
- Client merges results in ring order

---

### Networking and Concurrency
- XML-RPC used for inter-node communication
- Each node runs as a separate process
- Thread-safe using locks
- Avoids self-RPC calls to prevent deadlocks
