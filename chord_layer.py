import hashlib
import xmlrpc.client

class NetworkChordRing:
    """Routes DFS requests over the network via RPC."""
    def __init__(self, node_ports):
        # node_ports is a dict mapping node_id -> port
        self.ring_space = 2**160 
        self.sorted_node_ids = sorted(list(node_ports.keys()))
        
        # Connect to the remote RPC servers instead of local memory
        self.nodes = {}
        for node_id, port in node_ports.items():
            url = f"http://localhost:{port}"
            self.nodes[node_id] = xmlrpc.client.ServerProxy(url, allow_none=True)

    def hash_func(self, key_string):
        return int(hashlib.sha1(key_string.encode('utf-8')).hexdigest(), 16)

    def locate_successor(self, key_hash):
        for node_id in self.sorted_node_ids:
            if node_id >= key_hash:
                return node_id
        return self.sorted_node_ids[0] 

    def get_replica_group(self, key_hash, num_replicas=3):
        group = []
        start_idx = 0
        for i, node_id in enumerate(self.sorted_node_ids):
            if node_id >= key_hash:
                start_idx = i
                break
                
        for i in range(num_replicas):
            idx = (start_idx + i) % len(self.sorted_node_ids)
            group.append(self.sorted_node_ids[idx])
        return group

    # --- Replicated Network DHT Operations ---

    def put(self, key, value):
            replica_group = self.get_replica_group(key)
            import time
            # Cast timestamp to string to prevent XML-RPC 32-bit overflow
            t = str(int(time.time() * 1000)) 
            
            learn_count = 0
            for node_id in replica_group:
                rpc_proxy = self.nodes[node_id]
                # Send t as a string
                accepted = rpc_proxy.receive_accept("PUT operation", t)
                if accepted: learn_count += 1
                    
            if learn_count >= 2: 
                for node_id in replica_group:
                    # Cast key to string before sending
                    self.nodes[node_id].apply_commit("PUT", str(key), value, t)
            else:
                raise Exception(f"Paxos Write Failed for key {key}")

    def get(self, key):
        replica_group = self.get_replica_group(key)
        leader_id = replica_group[0]
        # Cast key to string before requesting
        return self.nodes[leader_id].get_data(str(key))

    def delete(self, key):
        replica_group = self.get_replica_group(key)
        import time
        t = str(int(time.time() * 1000))
        
        learn_count = 0
        for node_id in replica_group:
            if self.nodes[node_id].receive_accept("DELETE operation", t):
                learn_count += 1
                
        if learn_count >= 2:
            for node_id in replica_group:
                self.nodes[node_id].apply_commit("DELETE", str(key), None, t)