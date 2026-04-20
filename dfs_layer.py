import json
import os
from chord_layer import NetworkChordRing  # Importing the Network RPC layer

class DFS:
    """The Distributed File System abstraction layer."""
    def __init__(self, chord_ring, chunk_size=32):
        self.chord = chord_ring
        self.chunk_size = chunk_size
        self.root_dir_key = self.chord.hash_func("DFS_ROOT_DIR")
        
        # Initialize the root directory if it doesn't exist
        if self.chord.get(self.root_dir_key) is None:
            self.chord.put(self.root_dir_key, json.dumps([]))

    def _get_metadata_key(self, filename):
        return self.chord.hash_func(f"metadata:{filename}")

    def _get_page_key(self, filename, page_no):
        return self.chord.hash_func(f"{filename}:{page_no}")

    # --- PART A: CORE DFS API ---

    def ls(self):
        root_data = self.chord.get(self.root_dir_key)
        return json.loads(root_data)

    def touch(self, filename):
        meta_key = self._get_metadata_key(filename)
        if self.chord.get(meta_key) is not None:
            print(f"File '{filename}' already exists.")
            return

        metadata = {
            "filename": filename,
            "size_bytes": 0,
            "num_pages": 0,
            "pages": [],
            "version": 1
        }
        self.chord.put(meta_key, json.dumps(metadata))

        directory = self.ls()
        if filename not in directory:
            directory.append(filename)
            self.chord.put(self.root_dir_key, json.dumps(directory))

    def stat(self, filename):
        meta_key = self._get_metadata_key(filename)
        meta_data = self.chord.get(meta_key)
        if meta_data is None:
            raise FileNotFoundError(f"'{filename}' does not exist in DFS.")
        return json.loads(meta_data)

    def append(self, filename, local_path):
        metadata = self.stat(filename)
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file '{local_path}' not found.")

        with open(local_path, 'rb') as f:
            while True:
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break
                
                page_no = metadata["num_pages"]
                page_key = self._get_page_key(filename, page_no)
                
                self.chord.put(page_key, chunk.hex())
                
                metadata["pages"].append({
                    "page_no": page_no,
                    "guid": page_key,
                    "replicas": [] 
                })
                metadata["num_pages"] += 1
                metadata["size_bytes"] += len(chunk)

        metadata["version"] += 1
        self.chord.put(self._get_metadata_key(filename), json.dumps(metadata))

    def read(self, filename):
        metadata = self.stat(filename)
        file_data = bytearray()
        for page in metadata["pages"]:
            chunk_hex = self.chord.get(page["guid"])
            if chunk_hex is None:
                raise Exception(f"Data loss! Page {page['page_no']} missing.")
            file_data.extend(bytes.fromhex(chunk_hex))
        return file_data

    def head(self, filename, n_bytes):
        metadata = self.stat(filename)
        file_data = bytearray()
        for page in metadata["pages"]:
            chunk_hex = self.chord.get(page["guid"])
            file_data.extend(bytes.fromhex(chunk_hex))
            if len(file_data) >= n_bytes:
                break
        return file_data[:n_bytes]

    def tail(self, filename, n_bytes):
        metadata = self.stat(filename)
        file_data = bytearray()
        for page in reversed(metadata["pages"]):
            chunk_hex = self.chord.get(page["guid"])
            file_data = bytes.fromhex(chunk_hex) + file_data
            if len(file_data) >= n_bytes:
                break
        return file_data[-n_bytes:]

    def delete_file(self, filename):
        try:
            metadata = self.stat(filename)
        except FileNotFoundError:
            return

        for page in metadata["pages"]:
            self.chord.delete(page["guid"])
        self.chord.delete(self._get_metadata_key(filename))
        
        directory = self.ls()
        if filename in directory:
            directory.remove(filename)
            self.chord.put(self.root_dir_key, json.dumps(directory))

    # --- PART B: DISTRIBUTED SORT ---

    def sort_file(self, filename, output_filename):
        print(f"Starting distributed sort of '{filename}'...")
        
        try:
            raw_data = self.read(filename).decode('utf-8')
        except FileNotFoundError:
            print(f"Error: {filename} not found.")
            return

       # Scatter
        records = raw_data.split('\n')
        for record in records:
            if not record.strip():
                continue
            try:
                key, _ = record.split(',', 1)
            except ValueError:
                continue 
                
            # --- CHANGE IS HERE ---
            # Old: key_hash = self.chord.hash_func(key.strip())
            key_hash = self._order_preserving_map(key.strip())
            # ----------------------
            
            target_node_id = self.chord.locate_successor(key_hash)
            self.chord.nodes[target_node_id].insert_record(record)
        print("Routing complete. Triggering local node sorts...")

        # Gather
        temp_out_file = f"temp_{output_filename}"
        with open(temp_out_file, 'w') as f:
            for node_id in self.chord.sorted_node_ids:
                node = self.chord.nodes[node_id]
                sorted_local_records = node.local_sort()
                for k, v in sorted_local_records:
                    f.write(f"{k},{v}\n")
                node.clear_buffer()
        print(f"Assembly complete. Storing '{output_filename}' back into DFS...")

        # Store
        self.touch(output_filename)
        self.append(output_filename, temp_out_file)
        os.remove(temp_out_file)
        print("Distributed sort finished successfully!")

    def _order_preserving_map(self, key_string):
            """Maps a string to the ring space preserving alphabetical/numerical order."""
            # Pad string to 8 characters to ensure stable integer conversion
            padded_key = key_string.ljust(8, '\x00')[:8]
            # Convert the raw bytes of the string into a large integer
            key_int = int.from_bytes(padded_key.encode('utf-8'), 'big')
            # The maximum possible value for an 8-byte string
            max_val = (256 ** 8) - 1
            # Project this onto the Chord ring size
            return int((key_int / max_val) * self.chord.ring_space)
# ==========================================
# Execution / Testing Script
# ==========================================
if __name__ == "__main__":
    from chord_layer import NetworkChordRing
    import os
    
    node_ports = {
        0: 8000,
        292300327466180583640736966543256603931186508595: 8001,
        584600654932361167281473933086513207862373017190: 8002,
        876900982398541750922210899629769811793559525785: 8003,
        1169201309864722334562947866173026415724746034380: 8004
    }
    
    ring = NetworkChordRing(node_ports)
    dfs = DFS(ring, chunk_size=1024) 
    
    # 1. Clean the slate before testing!
    dfs.delete_file("data.csv")
    dfs.delete_file("sorted_data.csv")
    
    # 2. Add a newline \n to the very end of the test data
    test_file = "unsorted_test.csv"
    with open(test_file, "w") as f:
        f.write("0190,carol\n0042,bob\n0012,alice\n0999,zack\n0100,diana\n0350,eve\n")

    print("--- Loading Data (via Network RPC) ---")
    dfs.touch("data.csv")
    dfs.append("data.csv", test_file)
    print(dfs.read("data.csv").decode('utf-8').strip()) # strip() removes trailing empty lines

    print("\n--- Running Sort (via Network RPC) ---")
    dfs.sort_file("data.csv", "sorted_data.csv")
    
    print("\n--- Final Sorted Output ---")
    print(dfs.read("sorted_data.csv").decode('utf-8').strip())

    print("\n--- Phase 3: Inspecting Paxos Logs on Node 0 ---")
    try:
        log = ring.nodes[0].get_log()
        print("Paxos Log for Node 0:")
        for entry in log[-5:]: # Print the LAST 5 entries so you see the newest ones
            print("  - " + entry)
    except Exception as e:
        print(f"Could not fetch log: {e}")

    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)