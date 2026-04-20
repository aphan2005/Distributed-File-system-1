import sys
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import threading

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class PaxosNode:
    """An independent RPC Server representing a single peer."""
    def __init__(self, node_id):
        self.node_id = node_id
        self.dht_storage = {}
        self.sort_buffer = []  
        self.proposal_number = 0
        self.paxos_log = []
        # Thread lock to satisfy the Concurrency constraint
        self.lock = threading.Lock() 

    # --- Part B: Sorting Methods ---
    def insert_record(self, record_string):
        with self.lock:
            if "," in record_string:
                key, value = record_string.split(",", 1)
                self.sort_buffer.append((key.strip(), value.strip()))
        return True

    def local_sort(self):
        with self.lock:
            self.sort_buffer.sort(key=lambda x: x[0])
            # Return a copy to safely send over the network
            return list(self.sort_buffer) 
            
    def clear_buffer(self):
        with self.lock:
            self.sort_buffer.clear()
        return True

    # --- Part C: Paxos Methods ---
    def receive_accept(self, op_string, t):
        """Simulates receiving an ACCEPT over the network."""
        return True # Acknowledge (LEARN)

    def apply_commit(self, op_type, key, value, t):
        """Applies the committed operation."""
        with self.lock:
            self.paxos_log.append(f"Seq {t}: {op_type} for key {key}")
            if op_type == "PUT":
                self.dht_storage[key] = value
            elif op_type == "DELETE":
                if key in self.dht_storage:
                    del self.dht_storage[key]
        return True
        
    def get_data(self, key):
        """Allows clients to read data."""
        return self.dht_storage.get(key, None)
        
    def get_log(self):
        """Returns the Paxos log for grading checks."""
        return self.paxos_log

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python node_server.py <Node_ID> <Port>")
        sys.exit(1)
        
    node_id = int(sys.argv[1])
    port = int(sys.argv[2])
    
    server = SimpleXMLRPCServer(("localhost", port), requestHandler=RequestHandler, allow_none=True)
    server.register_instance(PaxosNode(node_id))
    
    print(f"Node {node_id} listening on port {port}...")
    server.serve_forever()