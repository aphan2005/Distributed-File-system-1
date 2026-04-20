
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

The project is organized into three primary modules, each representing a distinct layer of the distributed system stack:

```text
.
├── node_server.py      # The Storage Layer (Server-side)
├── chord_layer.py      # The Middleware Layer (Routing & Consensus)
└── dfs_layer.py        # The Application Layer (Client-side & Testing)
```

### 1. `node_server.py` (The Storage Layer)
This script implements the physical peer nodes. Each instance runs as an independent XML-RPC server process.

**Responsibilities:**
- **Local Storage:** Maintains the `dht_storage` dictionary for metadata and file pages.
- **Sort Buffer:** Manages a temporary `sort_buffer` used during the distributed MapReduce-style sort.
- **Paxos Logging:** Records every committed transaction into a human-readable `paxos_log` for auditing.
- **Concurrency:** Utilizes `SimpleXMLRPCServer` with threading to handle multiple simultaneous requests from the client.

### 2. `chord_layer.py` (The Middleware Layer)
This is the networking engine of the system. It abstracts the complexity of the distributed cluster into a single routing interface.

**Responsibilities:**
- **Chord Routing:** Implements `locate_successor` to map keys to specific node IDs on the 160-bit identifier ring.
- **Paxos Consensus:** Orchestrates the **ACCEPT** and **LEARN** phases across the 3-node replica group.
- **Quorum Logic:** Enforces the "majority rule," requiring 2 out of 3 nodes to acknowledge an operation before finalizing a commit.
- **Network Proxying:** Manages the RPC connections to the independent node servers.

### 3. `dfs_layer.py` (The Application Layer)
This is the user-facing entry point and client-side logic. It translates high-level file system commands into low-level DHT operations.

**Responsibilities:**
- **Metadata Management:** Tracking file versions, total size, and the list of page GUIDs.
- **File Chunking:** Slicing local files into fixed-size pages for distribution across the ring.
- **Distributed Sorting:** Executing the three-stage "Scatter-Sort-Gather" process using an **Order-Preserving Hash**.
- **Validation Suite:** Contains the main testing block that creates files, performs the sort, and verifies the Paxos logs.

---

## Technical Implementation Details

### 1. Chord-Based Distributed Routing
The system utilizes a **Chord-style Distributed Hash Table (DHT)** for decentralized data location.
- **Identifier Space:** We use a 160-bit identifier space ($0$ to $2^{160}-1$). Both nodes and data keys are hashed into this space using the **SHA-1 algorithm**.
- **Deterministic Mapping:** Data is stored using a "Successor" rule. When a file or metadata key is hashed, the system identifies the first node on the ring with an ID greater than or equal to the key's hash. This ensures that any client can find any piece of data without a central directory.

### 2. Simplified Paxos Consensus
To ensure strong consistency and handle potential node crashes, we implemented a simplified **Paxos protocol**.
- **Roles:** The node designated as the "Successor" by the Chord ring acts as the **Leader** for that specific key. The next two nodes in the ring act as **Followers**.
- **Commitment Flow:** 1. **Propose/Accept:** The leader assigns a unique, monotonically increasing sequence number ($t$) based on a millisecond timestamp.
    2. **Quorum Acknowledgment:** The leader sends an `ACCEPT(o, t)` message to the replica group. 
    3. **Learn:** Replicas acknowledge the operation. Once a **majority** (2 out of 3 nodes) responds, the operation is considered "Learned."
    4. **Commit:** The leader triggers an `apply_commit`, ensuring all replicas execute the operation in the exact same sequence order.

### 3. Metadata Management
The DFS treats file metadata as a first-class object in the DHT.
- **Deterministic Metadata Keys:** Metadata is stored under the key `hash("metadata:" + filename)`. 
- **Versioning:** Each metadata object contains a `version` number that increments with every file modification, allowing the system to track the lineage of file updates.
- **Chunking:** Large files are broken into smaller pages (chunks). The metadata stores a list of **GUIDs** (Global Unique Identifiers) for these pages, which are also stored deterministically using `hash(filename + ":" + page_number)`.

### 4. Distributed Sorting (MapReduce Pattern)
A key feature of this implementation is the **Order-Preserving Map** used for distributed sorting.
- **The Problem:** Standard SHA-1 hashing is designed to be random, which scatters alphabetically similar keys to completely different parts of the network, making global sorting impossible.
- **The Solution:** For the sorting phase, we replace SHA-1 with a custom projection function. This function converts the first 8 characters of a key into a numerical value and scales it to the $2^{160}$ ring space.
- **The Result:** Records are "shuffled" across the network so that specific nodes handle specific alphabetical ranges. This allows the system to gather sorted data simply by traversing the Chord ring in order.

### 5. Fault Model and Concurrency
- **Fault Tolerance:** The system assumes **Crash Failures** and no Byzantine behavior. By using a 3-node replica group, the system can lose any single node in a group and continue to provide consistent read/write access via the remaining majority.
- **Concurrency:** Each peer node runs as an independent process using Python’s `SimpleXMLRPCServer`. This provides a threaded environment where multiple RPC calls can be handled simultaneously, preventing a single slow network request from blocking the entire system.
