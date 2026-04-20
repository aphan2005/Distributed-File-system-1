
# Replicated Distributed File System (DFS)

## Overview
This project is a Distributed File System (DFS) built on a Chord-based Distributed Hash Table with strong consistency provided by a simplified Paxos consensus protocol. It supports **Distributed file stroage** using Chord lookups, **Page-based file metadata management**, and **Distributed sorting of file contents**. Chord assigns each key to the node responsible for its successor on the identifier ring, and uses finger tables to achieve logarithmic routing. Replication improves reliability and performance, but introduces consistency challenges. Paxos addresses the fault-tolerance side by ensuring that replicas execute operations in the same order despite crashes and lost or delayed messages.

## Required Protocol Concepts
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

## Setup & Execution Instructions

This is a distributed system using network sockets. You must start the background nodes first so the client has a network to connect to.

### 1. Start the Peer Nodes
Open **5 separate terminal windows** (or PowerShell windows) and run one of these commands in each. Leave these windows open while testing.

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

---

## Project Structure

* **`node_server.py`**: The individual peer node. It handles local storage, the sorting buffer, and maintains a Paxos log. It uses `xmlrpc.server` to handle concurrent requests.
* **`chord_layer.py`**: The middleware layer. It implements Chord ring routing and the Paxos `Propose -> Accept -> Learn -> Commit` workflow for replication.
* **`dfs_layer.py`**: The application layer. It handles file metadata, page chunking, and the distributed sorting logic. It acts as the client that coordinates with the node servers.

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
