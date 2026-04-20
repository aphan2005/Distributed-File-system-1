
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

- **`node_server.py`**: The individual peer node. It handles local storage, the sorting buffer, and maintains a Paxos log. It uses `xmlrpc.server` to handle concurrent requests.
- **`chord_layer.py`**: The middleware layer. It implements Chord ring routing and the Paxos `Propose -> Accept -> Learn -> Commit` workflow for replication.
- * **`dfs_layer.py`**: The application layer. It handles file metadata, page chunking, and the distributed sorting logic. It acts as the client that coordinates with the node servers.



## Technical Implementation Details

### 1. Paxos Consensus
For every write operation (metadata update or page append), the system uses a majority-based commitment. With 3 replicas per object, the system requires at least 2 nodes to acknowledge an operation before it is committed to the logs.

### 2. Distributed Sorting
The system uses an **Order-Preserving Hash Map** instead of standard SHA-1 for the sorting keys. This ensures that when records are routed to different peers in the Chord ring, they remain in a relative order that allows for a globally sorted assembly when reading the ring sequentially.

### 3. Fault Tolerance & Consistency
* **Fault Model**: Handles crash failures where nodes can stop responding. As long as a majority of a replica group is alive, data remains available and consistent.
* **Consistency**: All replicas apply committed operations in the same order using strictly increasing sequence numbers derived from timestamps.
* **Communication**: Implemented using XML-RPC to satisfy requirements for distributed messaging and concurrency.
```
