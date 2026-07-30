from chord_hash import chord_hash, in_interval, short_id, M


# A lookup should take O(log N) hops. This limit only exists to avoid bouncing forever.
MAX_HOPS = 64
# How many consecutive failed pings before a node declares its predecessor dead.
FAILED_PINGS_BEFORE_DROP = 3


class NodeRef:
    """
    Reference to a node: its id and address.
    This is stored in successor/predecessor/finger table slots,
    instead of a full Node object.
    """
    def __init__(self, address, m= M):
        self.address = address
        self.id = chord_hash(address, m)

    def __repr__(self):
        return f"NodeRef(id={self.id}, addr={self.address})"


class Node:
    def __init__(self, address, network, m=M, num_replicas=2, log_fn=None):
        self.m = m
        self.ref = NodeRef(address, m)
        self.id = self.ref.id
        self.network = network
        self.num_replicas = num_replicas  # size of the successor list
        self._log = log_fn or (lambda msg: None)  # silent if no logger was given

        # A single node in the ring is its own successor, and has no predecessor yet.
        self.successor = self.ref
        self.predecessor = None
        self.successor_list = [self.ref]  # for fault tolerance

        # Finger table: m entries, all pointing to self initially (updated on fix_fingers).
        self.finger_table = [self.ref for _ in range(m)]
        self.next_finger = 0  # rotates each time fix_figners runs

        # Local key-value store: {"username", "status"}
        self.data = {}
        # Backup copies of other predecessors' keys.
        self.replicas = {}
        # count failed pings to this node's predecessor
        self.failed_pings = 0
        # nodes are holding backups of this node's keys
        self.replica_targets = {}

    def find_successor(self, id_, hops=0):
        """Finds who is responsible for the id."""
        # The id is in this node's range, so the responsible for it is its successor.
        if in_interval(id_, self.id, self.successor.id, self.m, inclusive_end=True):
            return self.successor

        if hops >= MAX_HOPS:
            return self.ref

        # Try candidates from farthest finger to nearest,
        # if one is dead try the next best jump.
        for candidate in self.closest_preceding_candidates(id_):
            try:
                return self.network.send(candidate.address, "find_successor", id_, hops + 1)
            except ConnectionError:
                continue

        # Every finger is unreachable. Try with each node in the successors list
        for succ in self.successor_list:
            if succ.id == self.id:
                continue
            try:
                return self.network.send(succ.address, "find_successor", id_, hops + 1)
            except ConnectionError:
                continue

        return self.ref

    def closest_preceding_candidates(self, id_):
        """
        Every finger table entry that precedes id_, ordered from farthest to nearest.
        """
        seen = set()
        candidates = []
        for finger in reversed(self.finger_table):
            if finger.id == self.id or finger.id in seen:
                continue
            if in_interval(finger.id, self.id, id_, self.m, inclusive_end=False):
                seen.add(finger.id)
                candidates.append(finger)
        return candidates

    def join(self, existing_address):
        """
        Join the ring through a node that is already a member.
        """
        self.predecessor = None
        self.successor = self.network.send(existing_address, "find_successor", self.id)

    def hand_off_keys(self):
        """
        Give the keys this node no longer covers, to its predecessor.
        """
        if self.predecessor is None:
            return
        # check if there is keys not belongs to this node
        payload = []
        for key, entry in list(self.data.items()):
            if not in_interval(key, self.predecessor.id, self.id, self.m, inclusive_end=True):
                payload.append([key, entry["username"], entry["status"]])
        if not payload:
            return

        try:
            self.network.send(self.predecessor.address, "receive_keys", payload)
        except ConnectionError:
            return  # predecessor unreachable: keep the keys and retry next cycle

        # Only drop them once the new owner has confirmed it stored them
        self._log(f"..{short_id(self.predecessor.id)} cubre ahora {len(payload)} clave(s)")
        usernames = []
        for key, username, status in payload:
            self.data.pop(key, None)
            usernames.append(username)

        # The backups still tagged under this node's id have to be deleted
        for succ in self.successor_list:
            if succ.id == self.id:
                continue
            try:
                self.network.send(succ.address, "drop_replica", usernames, self.ref)
            except ConnectionError:
                pass

    def receive_keys(self, entries):
        """
        Accept keys sended from the node that was covering this range.
        """
        for key, username, status in entries:
            self.data[key] = {"username": username, "status": status}
            self.replicate(username, status)

    VALID_STATUS = {"connected", "disconnected"}

    def write(self, username, status):
        """Entry point a client can call on any node: write(k,v). Stores a user status."""
        if status not in self.VALID_STATUS:
            raise ValueError(f"invalid status: {status!r} (must be 'connected' or 'disconnected')")
        key = chord_hash(username, self.m)
        owner = self.find_successor(key)
        self._log(f"cliente solicita write({username}) -> reenviando a owner ..{short_id(owner.id)}")

        try:
            self.network.send(owner.address, "write_local", username, status)
        except ConnectionError:
            # Owner died while writting, try again.
            self._log(f"owner ..{short_id(owner.id)} cayo durante write({username}) -> reintentando")
            retry_owner = self.find_successor(key)
            self.network.send(retry_owner.address, "write_local", username, status)

    def write_local(self, username, status):
        """Stores in this node's primary table and replicates the backup to its successors."""
        if status not in self.VALID_STATUS:
            raise ValueError(f"invalid status: {status!r} (must be connected or disconnected)")
        key = chord_hash(username, self.m)
        self._log(f"write_local: {username} -> {status}")
        self.data[key] = {"username": username, "status": status}
        self.replicate(username, status)

    def replicate(self, username, status):
        """Pushes a backup copy of this key to its successor list."""
        for succ in self.successor_list:
            if succ.id != self.id:
                try:
                    self.network.send(succ.address, "store_replica", username, status, self.ref)
                except ConnectionError:
                    pass  # successor is down

    def store_replica(self, username, status, owner):
        """Stores a backup copy, separate from its own primary data."""
        key = chord_hash(username, self.m)
        self.replicas[key] = {"username": username, "status": status, "owner": owner.id}

    def sync_replicas(self):
        """
        Runs at the end of stabilize(). It works just if the successor list changed.
        """
        targets = {s.id: s for s in self.successor_list if s.id != self.id} # who has to have these node's copies? (actual successors)
        if targets.keys() == self.replica_targets.keys(): # actual successors =? suppossed actual succesors
            return

        entries = list(self.data.values())
        if entries:
            for id_ in targets.keys() - self.replica_targets.keys():
                ref = targets[id_]   # just entered the list, so it has none of these keys
                self._log(f"..{short_id(id_)} entró a mi successor list -> replicando {len(entries)} clave(s)")
                for e in entries:
                    try:
                        self.network.send(ref.address, "store_replica", e["username"], e["status"], self.ref)
                    except ConnectionError:
                        break  # it just died; the next stabilize will drop it from the list

            usernames = [e["username"] for e in entries]
            for id_ in self.replica_targets.keys() - targets.keys():
                ref = self.replica_targets[id_]  # left the list, its copies are stale now
                try:
                    self.network.send(ref.address, "drop_replica", usernames, self.ref)
                    self._log(f"..{short_id(id_)} salió de mi successor list -> descartando mis replicas ahi")
                except ConnectionError:
                    pass

        self.replica_targets = targets

    def drop_replica(self, usernames, owner):
        """
        Called on a replica holder when a node no longer owns these keys (a new node
        joined and have that slice of the ring).
        """
        for username in usernames:
            key = chord_hash(username, self.m)
            existing = self.replicas.get(key)
            if existing and existing["owner"] == owner.id:
                self.replicas.pop(key, None)

    def read(self, username):
        """Entry point a client can call on any node: read(k). Check a user status."""
        key = chord_hash(username, self.m)
        owner = self.find_successor(key)
        try:
            return self.network.send(owner.address, "read_local", username)
        except ConnectionError:
            # The owner died and nobody has noticed yet, ask successors to read replicas.
            self._log(f"owner ..{short_id(owner.id)} no responde para read({username}) -> buscando replica")
            return self.read_from_replica_holders(username, owner)

    def read_from_replica_holders(self, username, dead_owner):
        probe = dead_owner.id
        for _ in range(self.num_replicas):
            holder = self.find_successor((probe + 1) % (2 ** self.m))
            if holder.id == dead_owner.id:
                break
            try:
                value = self.network.send(holder.address, "read_local", username)
            except ConnectionError:
                value = None
            if value is not None:
                return value
            probe = holder.id
        return None

    def read_local(self, username):
        """
        Answer from this node's primary data.
        """
        key = chord_hash(username, self.m)
        entry = self.data.get(key) or self.replicas.get(key)
        return entry["status"] if entry else None

    def get_predecessor(self):
        return self.predecessor

    def notify(self, candidate):
        """Another node thinks it might be this node's predecessor."""
        if self.predecessor is None or in_interval(candidate.id, self.predecessor.id, self.id, self.m):
            if self.predecessor is None or self.predecessor.id != candidate.id:
                self._log(f"predecessor -> ..{short_id(candidate.id)}")
            self.predecessor = candidate # this node has a new predecessor

    def stabilize(self):
        """
        Checks whether the predecessor of the successor is a better option
        and refresh the successor list. If the successor turns out to be dead,
        promote the next candidate in the successor list.
        """
        while True:
            try:
                x = self.network.send(self.successor.address, "get_predecessor")
                succ_list = self.network.send(self.successor.address, "get_successor_list")
                break
            except ConnectionError:
                if len(self.successor_list) > 1:
                    # successor is dead. Drop it and promote the next one
                    dead_id = self.successor.id
                    self.successor_list.pop(0)
                    self.successor = self.successor_list[0]
                    self._log(f"successor ..{short_id(dead_id)} caido -> promoviendo successor ..{short_id(self.successor.id)}")
                else:
                    # no fallback known
                    self._log("successor caido y no hay fallback conocido (ring degradado)")
                    self.successor = self.ref
                    self.successor_list = [self.ref]
                    return
                
        if x is not None and in_interval(x.id, self.id, self.successor.id, self.m) and x.id != self.successor.id:
            # Adopt x, that is now between this node and the old successor and
            # fetch the list that corresponds to the new node
            try:
                x_list = self.network.send(x.address, "get_successor_list")
            except ConnectionError:
                x_list = None
            if x_list is not None:
                self._log(f"successor -> ..{short_id(x.id)}")
                self.successor = x
                succ_list = x_list

        self.successor_list = [self.successor] + succ_list[: self.num_replicas - 1]

        try:
            self.network.send(self.successor.address, "notify", self.ref)
        except ConnectionError:
            pass

        self.hand_off_keys()
        self.sync_replicas()

    def get_successor_list(self):
        return self.successor_list

    def check_predecessor(self):
        """
        Heartbeat to the predecessor. If it is dead, forget it and promote the
        replicas this node was holding for it into its own primary data, since this
        node is now the owner of that key range.
        """
        if self.predecessor is None:
            return
        try:
            self.network.send(self.predecessor.address, "ping")
            self.failed_pings = 0
            return
        except ConnectionError:
            self.failed_pings += 1
            if self.failed_pings < FAILED_PINGS_BEFORE_DROP:
                return

        dead = self.predecessor # predecessor died, then this node takes his place
        self.predecessor = None
        self.failed_pings = 0

        # Only the replicas that belonged to the dead node 
        inherited = {k: v for k, v in self.replicas.items() if v["owner"] == dead.id}

        if inherited:
            self._log(f"predecessor ..{short_id(dead.id)} caido -> promoviendo {len(inherited)} clave(s) de replica a dato primario")
        else:
            self._log(f"predecessor ..{short_id(dead.id)} caido")

        # Now replicas are the real data for this node
        for key, entry in inherited.items():
            self.replicas.pop(key, None)
            self.data[key] = {"username": entry["username"], "status": entry["status"]}

        # Replicate keys to the new successors
        for entry in inherited.values():
            self.replicate(entry["username"], entry["status"])

    def ping(self):
        return True

    def fix_fingers(self):
        """
        Refresh one finger table entry per call.
        """
        i = self.next_finger
        start = (self.id + 2 ** i) % (2 ** self.m)
        self.finger_table[i] = self.find_successor(start)
        self.next_finger = (self.next_finger + 1) % self.m

    def __repr__(self):
        return (f"Node(id={self.id}, addr={self.ref.address}, "
                f"successor={self.successor.id}, "
                f"predecessor={self.predecessor.id if self.predecessor else None})")

    def get_debug_state(self):
        """View of this node's state."""
        return {
            "id": self.id,
            "successor": self.successor.id,
            "successor_list": [s.id for s in self.successor_list],
            "predecessor": self.predecessor.id if self.predecessor else None,
            "data": {v["username"]: v["status"] for v in self.data.values()},
            "replicas": {v["username"]: v["status"] for v in self.replicas.values()},
        }
