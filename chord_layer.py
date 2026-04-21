import hashlib
import xmlrpc.client

RING_BITS = 160
RING_SPACE = 2 ** RING_BITS

DEFAULT_NODE_PORTS = {
    0: 8000,
    292300327466180583640736966543256603931186508595: 8001,
    584600654932361167281473933086513207862373017190: 8002,
    876900982398541750922210899629769811793559525785: 8003,
    1169201309864722334562947866173026415724746034380: 8004,
}


def sha1_int(text):
    """
    Generates a 160-bit integer from a string using the SHA-1 hashing algorithm.
    This serves as the primary hash function for consistent mapping within 
    the Chord DHT.
    """
    return int(hashlib.sha1(text.encode("utf-8")).hexdigest(), 16)


class NetworkChordRing:
    def __init__(self, node_ports):
        """
        Initializes the Chord middleware by mapping node IDs to their network ports 
        and establishing XML-RPC proxies for remote communication across the ring.
        """
        self.ring_space = RING_SPACE
        self.node_ports = dict(node_ports)
        self.sorted_node_ids = sorted(self.node_ports.keys())
        self.nodes = {
            node_id: xmlrpc.client.ServerProxy(
                f"http://localhost:{port}", allow_none=True
            )
            for node_id, port in self.node_ports.items()
        }

    def hash_func(self, key_string):
        """
        Deterministically maps a string key to a 160-bit integer 
        representing its coordinate in the Chord identifier space.
        """
        return sha1_int(key_string)

    def locate_successor(self, key_hash):
        """
        Identifies the node responsible for a given hash. Implements the 
        Chord 'successor' rule: the node with the smallest ID greater than 
        or equal to the hash, wrapping around to the first node if necessary.
        """
        for node_id in self.sorted_node_ids:
            if node_id >= key_hash:
                return node_id
        return self.sorted_node_ids[0]

    def get_replica_group(self, key_hash, num_replicas=3):
        """
        Calculates the subset of nodes responsible for maintaining high 
        availability for a key. Returns the primary successor (Leader) 
        and the subsequent nodes in the ring (Followers).
        """
        leader_id = self.locate_successor(key_hash)
        start_index = self.sorted_node_ids.index(leader_id)
        replica_count = min(num_replicas, len(self.sorted_node_ids))
        return [
            self.sorted_node_ids[(start_index + offset) % len(self.sorted_node_ids)]
            for offset in range(replica_count)
        ]

    def put(self, key, value):
        """
        Locates the successor node (Leader) for the given key and triggers 
         a replicated storage operation across the replica group.
        """
        leader_id = self.locate_successor(key)
        return self.nodes[leader_id].client_put(str(key), value)

    def get(self, key):
        """
        Identifies the node responsible for a key and retrieves the associated 
        data from the primary replica.
        """
        leader_id = self.locate_successor(key)
        return self.nodes[leader_id].client_get(str(key))

    def delete(self, key):
        """
        Locates the node managing the key and initiates a coordinated 
        removal of the data across all replicas.
        """
        leader_id = self.locate_successor(key)
        return self.nodes[leader_id].client_delete(str(key))

    def clear_sort_buffers(self):
        """
        Commands all nodes in the Chord ring to purge their local sort buffers
        to release resources and prepare for future sorting operations.
        """
        for node_id in self.sorted_node_ids:
            try:
                self.nodes[node_id].clear_buffer()
            except Exception:
                pass

    def collect_sorted_records(self):
        """
        Traverses the Chord ring in order to aggregate locally sorted records 
        from each node, resulting in a globally sorted dataset.
        """
        records = []
        for node_id in self.sorted_node_ids:
            try:
                local_records = self.nodes[node_id].local_sort()
                records.extend(local_records)
            except Exception:
                pass
        return records
