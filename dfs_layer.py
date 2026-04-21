import json
import os

from chord_layer import DEFAULT_NODE_PORTS, NetworkChordRing


class DFS:
    def __init__(self, chord_ring, chunk_size=1024):
        self.chord = chord_ring
        self.chunk_size = chunk_size
        self.root_dir_key = self.chord.hash_func("DFS_ROOT_DIR")

        if self.chord.get(self.root_dir_key) is None:
            self.chord.put(self.root_dir_key, json.dumps([]))

    def _get_metadata_key(self, filename):
        return self.chord.hash_func(f"metadata:{filename}")

    def _get_page_key(self, filename, page_no):
        return self.chord.hash_func(f"{filename}:{page_no}")

    def ls(self):
        raw = self.chord.get(self.root_dir_key)
        return [] if raw is None else json.loads(raw)

    def touch(self, filename):
        meta_key = self._get_metadata_key(filename)
        if self.chord.get(meta_key) is not None:
            return False

        metadata = {
            "filename": filename,
            "size_bytes": 0,
            "num_pages": 0,
            "pages": [],
            "version": 1,
        }
        self.chord.put(meta_key, json.dumps(metadata))

        directory = self.ls()
        if filename not in directory:
            directory.append(filename)
            directory.sort()
            self.chord.put(self.root_dir_key, json.dumps(directory))
        return True

    def stat(self, filename):
        meta_key = self._get_metadata_key(filename)
        raw = self.chord.get(meta_key)
        if raw is None:
            raise FileNotFoundError(f"'{filename}' does not exist in DFS.")
        return json.loads(raw)

    def append(self, filename, local_path):
        metadata = self.stat(filename)
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file '{local_path}' not found.")

        with open(local_path, "rb") as source:
            while True:
                chunk = source.read(self.chunk_size)
                if not chunk:
                    break

                page_no = metadata["num_pages"]
                page_key = self._get_page_key(filename, page_no)
                replica_ids = self.chord.get_replica_group(page_key)

                self.chord.put(page_key, chunk.hex())
                metadata["pages"].append({
                    "page_no": page_no,
                    "guid": str(page_key),
                    "replicas": [str(node_id) for node_id in replica_ids],
                })
                metadata["num_pages"] += 1
                metadata["size_bytes"] += len(chunk)

        metadata["version"] += 1
        self.chord.put(self._get_metadata_key(filename), json.dumps(metadata))

    def read(self, filename):
        metadata = self.stat(filename)
        result = bytearray()

        for page in metadata["pages"]:
            raw_chunk = self.chord.get(int(page["guid"]))
            if raw_chunk is None:
                raise Exception(f"Data loss detected: page {page['page_no']} is unavailable.")
            result.extend(bytes.fromhex(raw_chunk))

        return bytes(result)

    def head(self, filename, n):
        return self.read(filename)[:n]

    def tail(self, filename, n):
        return self.read(filename)[-n:]

    def delete_file(self, filename):
        try:
            metadata = self.stat(filename)
        except FileNotFoundError:
            return False

        for page in metadata["pages"]:
            self.chord.delete(int(page["guid"]))
        self.chord.delete(self._get_metadata_key(filename))

        directory = self.ls()
        if filename in directory:
            directory.remove(filename)
            self.chord.put(self.root_dir_key, json.dumps(directory))
        return True

    def sort_file(self, filename, output_filename):
        raw_text = self.read(filename).decode("utf-8")
        self.chord.clear_sort_buffers()

        for line in raw_text.splitlines():
            if not line.strip() or "," not in line:
                continue
            key, _ = line.split(",", 1)
            target_hash = self._order_preserving_map(key.strip())
            target_id = self.chord.locate_successor(target_hash)
            self.chord.nodes[target_id].insert_record(line)

        ordered_records = self.chord.collect_sorted_records()

        temp_path = f"temp_{output_filename}"
        with open(temp_path, "w", encoding="utf-8") as handle:
            for key, value in ordered_records:
                handle.write(f"{key},{value}\n")

        self.delete_file(output_filename)
        self.touch(output_filename)
        self.append(output_filename, temp_path)
        os.remove(temp_path)

        self.validate_sorted_output(output_filename)

    def validate_sorted_output(self, filename):
        raw_text = self.read(filename).decode("utf-8")
        keys = []
        for line in raw_text.splitlines():
            if not line.strip() or "," not in line:
                continue
            key, _ = line.split(",", 1)
            keys.append(key.strip())

        if any(keys[i] > keys[i + 1] for i in range(len(keys) - 1)):
            raise AssertionError(f"Sorted output validation failed for '{filename}'.")
        return True

    def _order_preserving_map(self, key_string):
        padded = key_string.ljust(8, "\x00")[:8]
        key_int = int.from_bytes(padded.encode("utf-8"), "big")
        max_val = (256 ** 8) - 1
        return int((key_int / max_val) * self.chord.ring_space)


if __name__ == "__main__":
    print("STARTING DFS")
    ring = NetworkChordRing(DEFAULT_NODE_PORTS)

    print("CREATING DFS OBJECT")
    dfs = DFS(ring, chunk_size=1024)

    print("DFS CREATED")

    test_input = "unsorted_test.csv"
    with open(test_input, "w", encoding="utf-8") as handle:
        handle.write("0190,carol\n0042,bob\n0012,alice\n0999,zack\n0100,diana\n0350,eve\n")

    dfs.delete_file("data.csv")
    dfs.delete_file("sorted_data.csv")

    print("--- Ring Diagnostics ---")
    for node_id in ring.sorted_node_ids:
        try:
            info = ring.nodes[node_id].get_ring_info()
            print(info)
        except Exception as exc:
            print(f"Could not read ring info from {node_id}: {exc}")

    print("\n--- Loading data.csv ---")
    dfs.touch("data.csv")
    dfs.append("data.csv", test_input)
    print(dfs.read("data.csv").decode("utf-8").strip())

    print("\n--- Sorting into sorted_data.csv ---")
    dfs.sort_file("data.csv", "sorted_data.csv")
    print(dfs.read("sorted_data.csv").decode("utf-8").strip())

    print("\n--- Validation ---")
    print("sorted_data.csv validated:", dfs.validate_sorted_output("sorted_data.csv"))

    print("\n--- Paxos / replication logs (tail) ---")
    for node_id in ring.sorted_node_ids[:3]:
        try:
            log = ring.nodes[node_id].get_log()
            print(f"Node {node_id} log tail:")
            for entry in log[-8:]:
                print("  ", entry)
        except Exception as exc:
            print(f"Could not fetch log from {node_id}: {exc}")

    if os.path.exists(test_input):
        os.remove(test_input)
