import sys
import threading
import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCRequestHandler, SimpleXMLRPCServer

DEFAULT_NODE_PORTS = {
    0: 8000,
    292300327466180583640736966543256603931186508595: 8001,
    584600654932361167281473933086513207862373017190: 8002,
    876900982398541750922210899629769811793559525785: 8003,
    1169201309864722334562947866173026415724746034380: 8004,
}


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ("/RPC2",)


class PaxosNode:
    def __init__(self, node_id, port, node_ports=None):
        self.node_id = node_id
        self.port = port
        self.node_ports = dict(node_ports or DEFAULT_NODE_PORTS)
        self.sorted_node_ids = sorted(self.node_ports.keys())
        self.node_rank = self.sorted_node_ids.index(self.node_id)

        self.dht_storage = {}
        self.sort_buffer = []

        self.ballot_counter = 0
        self.highest_promised = {}
        self.learned_ops = {}

        self.paxos_log = []
        self.lock = threading.RLock()
        self._proxies = {}

    def _proxy(self, node_id):
        proxy = self._proxies.get(node_id)
        if proxy is None:
            proxy = xmlrpc.client.ServerProxy(
                f"http://localhost:{self.node_ports[node_id]}",
                allow_none=True
            )
            self._proxies[node_id] = proxy
        return proxy

    def _append_log(self, message):
        timestamp = time.strftime("%H:%M:%S")
        self.paxos_log.append(f"[{timestamp}] node={self.node_id} {message}")

    def _locate_successor(self, key_hash):
        for node_id in self.sorted_node_ids:
            if node_id >= key_hash:
                return node_id
        return self.sorted_node_ids[0]

    def _replica_group_for_key(self, key_hash, replicas=3):
        leader_id = self._locate_successor(key_hash)
        start_index = self.sorted_node_ids.index(leader_id)
        replica_count = min(replicas, len(self.sorted_node_ids))
        return [
            self.sorted_node_ids[(start_index + offset) % len(self.sorted_node_ids)]
            for offset in range(replica_count)
        ]

    def _new_ballot(self):
        with self.lock:
            self.ballot_counter += 1
            return f"{self.ballot_counter}:{self.node_rank}"

    def _new_unique_slot(self):
        # STRING to avoid XML-RPC integer overflow
        return f"{time.time_ns()}:{self.node_rank}"

    def _parse_ballot(self, ballot):
        counter, rank = ballot.split(":", 1)
        return int(counter), int(rank)

    def _ballot_ge(self, left, right):
        return self._parse_ballot(left) >= self._parse_ballot(right)

    def client_put(self, key, value):
        key_hash = int(key)
        replica_group = self._replica_group_for_key(key_hash)

        if self.node_id != replica_group[0]:
            return self._proxy(replica_group[0]).client_put(key, value)

        ballot = self._new_ballot()
        slot = self._new_unique_slot()
        op = {"op_type": "PUT", "key": key, "value": value}

        self._append_log(
            f"LEADER begin slot={slot} ballot={ballot} op=PUT key={key} replicas={replica_group}"
        )

        accept_count = 0
        majority = (len(replica_group) // 2) + 1

        for replica_id in replica_group:
            try:
                if replica_id == self.node_id:
                    accepted = self.receive_accept(slot, ballot, op)
                else:
                    accepted = self._proxy(replica_id).receive_accept(slot, ballot, op)

                if accepted:
                    accept_count += 1
            except Exception as exc:
                self._append_log(f"ACCEPT failure slot={slot} replica={replica_id} error={exc}")

        if accept_count < majority:
            raise Exception(f"Majority ACCEPT failed for key {key}")

        for replica_id in replica_group:
            try:
                if replica_id == self.node_id:
                    self.receive_learn(slot, ballot, op)
                else:
                    self._proxy(replica_id).receive_learn(slot, ballot, op)
            except Exception as exc:
                self._append_log(f"LEARN failure slot={slot} replica={replica_id} error={exc}")

        self._append_log(f"COMMIT slot={slot} ballot={ballot} key={key}")
        return True

    def client_get(self, key):
        key_hash = int(key)
        replica_group = self._replica_group_for_key(key_hash)

        for replica_id in replica_group:
            try:
                if replica_id == self.node_id:
                    value = self.get_local_data(key)
                else:
                    value = self._proxy(replica_id).get_local_data(key)
                if value is not None:
                    return value
            except Exception:
                continue
        return None

    def client_delete(self, key):
        key_hash = int(key)
        replica_group = self._replica_group_for_key(key_hash)

        if self.node_id != replica_group[0]:
            return self._proxy(replica_group[0]).client_delete(key)

        ballot = self._new_ballot()
        slot = self._new_unique_slot()
        op = {"op_type": "DELETE", "key": key, "value": None}

        accept_count = 0
        majority = (len(replica_group) // 2) + 1

        for replica_id in replica_group:
            try:
                if replica_id == self.node_id:
                    accepted = self.receive_accept(slot, ballot, op)
                else:
                    accepted = self._proxy(replica_id).receive_accept(slot, ballot, op)

                if accepted:
                    accept_count += 1
            except Exception:
                pass

        if accept_count < majority:
            raise Exception(f"Majority ACCEPT failed for delete key {key}")

        for replica_id in replica_group:
            try:
                if replica_id == self.node_id:
                    self.receive_learn(slot, ballot, op)
                else:
                    self._proxy(replica_id).receive_learn(slot, ballot, op)
            except Exception:
                pass

        return True

    def receive_accept(self, slot, ballot, op):
        with self.lock:
            promised = self.highest_promised.get(slot)
            if promised is None or self._ballot_ge(ballot, promised):
                self.highest_promised[slot] = ballot
                self._append_log(
                    f"ACCEPTED slot={slot} ballot={ballot} op={op['op_type']} key={op['key']}"
                )
                return True
            return False

    def receive_learn(self, slot, ballot, op):
        with self.lock:
            promised = self.highest_promised.get(slot)
            if promised is not None and not self._ballot_ge(ballot, promised):
                return False

            self.highest_promised[slot] = ballot
            self.learned_ops[slot] = op
            self._append_log(
                f"LEARNED slot={slot} ballot={ballot} op={op['op_type']} key={op['key']}"
            )

            op_type = op["op_type"]
            key = op["key"]
            value = op.get("value")

            if op_type == "PUT":
                self.dht_storage[key] = value
            elif op_type == "DELETE":
                self.dht_storage.pop(key, None)

            self._append_log(f"APPLIED slot={slot} op={op_type} key={key}")
            return True

    def get_local_data(self, key):
        with self.lock:
            return self.dht_storage.get(key)

    def get_log(self):
        with self.lock:
            return list(self.paxos_log)

    def get_ring_info(self):
        return {
            "node_id": str(self.node_id),
            "port": self.port,
            "sorted_nodes": [str(node_id) for node_id in self.sorted_node_ids],
        }

    def insert_record(self, record_string):
        with self.lock:
            if "," not in record_string:
                return False
            key, value = record_string.split(",", 1)
            self.sort_buffer.append([key.strip(), value.strip()])
            return True

    def local_sort(self):
        with self.lock:
            self.sort_buffer.sort(key=lambda pair: pair[0])
            return list(self.sort_buffer)

    def clear_buffer(self):
        with self.lock:
            self.sort_buffer.clear()
        return True


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python node_server.py <Node_ID> <Port>")
        sys.exit(1)

    node_id = int(sys.argv[1])
    port = int(sys.argv[2])

    server = SimpleXMLRPCServer(
        ("localhost", port),
        requestHandler=RequestHandler,
        allow_none=True,
        logRequests=False,
    )
    server.register_introspection_functions()
    server.register_instance(PaxosNode(node_id, port))

    print(f"Node {node_id} listening on port {port}...")
    server.serve_forever()
