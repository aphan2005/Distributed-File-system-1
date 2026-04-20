# Distributed-File-system-1

## Overview
This project implements a distributed application-level storage system built on top of a Chord-like Distributed Hash Table (DHT). It extends basic key-value routing into a fully featured Distributed File System (DFS) capable of storing, retrieving, distributed sorting, and replicating file data across multiple peer nodes.

The system is highly fault-tolerant, utilizing a simplified **Paxos consensus protocol** to ensure strong consistency across replicas even in the presence of node crashes or message delays.

## Project Architecture

The project is strictly modular, separating network communication, routing, and application logic:

1. **`node_server.py` (The Peer Node)**
   - Acts as an independent server process using Python's built-in `xmlrpc.server`.
   - Maintains local state for the DHT storage, a sort buffer, and a human-readable Paxos log.
   - Designed to run concurrently across multiple terminal windows (or machines) to simulate a real network.

2. **`chord_layer.py` (The Middleware)**
   - Implements the Chord routing logic (`locate_successor`) to map keys to physical network ports.
   - Manages the **Paxos Consensus** flow (Propose -> Accept -> Learn -> Commit).
   - Replicates all data to a quorum of 3 nodes (Leader + 2 Followers).

3. **`dfs_layer.py` (The Client Application)**
   - The user-facing application layer.
   - Manages File Metadata (`size`, `num_pages`, `version`).
   - Implements file chunking and MapReduce-style distributed sorting using an **Order-Preserving Hash Function**.

## Features
* **DFS Operations:** `touch`, `append`, `read`, `head`, `tail`, `ls`, `stat`, `delete`.
* **Distributed Sort:** Reads a file, routes records to responsible peers based on an order-preserving hash, performs local node sorting, and gathers the globally sorted output.
* **Fault Tolerance (Paxos):** Every file page and metadata update requires a majority vote (2/3 nodes) to commit, ensuring data survives node failures.

---

## Prerequisites
* Python 3.x
* No external libraries are required. The project relies entirely on Python standard libraries (`hashlib`, `json`, `os`, `xmlrpc`, `threading`).

---

## How to Run the System

Because this is a true distributed network simulation, you **must** start the server nodes before running the client application.

### Step 1: Start the Server Nodes
Open **5 separate terminal windows** (or PowerShell/Command Prompts). Leave these running in the background.

Run one of the following commands in each window to start the nodes on ports 8000-8004:

**Terminal 1:**
