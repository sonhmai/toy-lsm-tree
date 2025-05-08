import json
import os
import pickle
from typing import Dict, Any

from lsm.wal_entry import WalEntry

class WalStore:
    def __init__(self, data_file: str, wal_file: str) -> None:
        self.data_file = data_file # where we save temp data periodically during checkpoint like a db backup
        self.wal_file = wal_file # transaction log
        self.data: Dict[str, Any] = {}
        self._recover()

    def _append_wal(self, entry: WalEntry):
        """ make sure that an entry is persisted to storage """
        try:
            with open(self.wal_file, "a") as f:
                f.write(entry.serialize() + "\n") # this only copies to Python buffer
                f.flush() # push python in-process buffer to OS page cache
                os.fsync(f.fileno())
        except IOError as e:
            raise RuntimeError(f"Failed to write to WAL: {e}")

    def _recover(self):
        """ 
        Rebuild database state from WAL 
            1. load last saved state from data file.
            2. replay all operations from wal onto that state.
            3. handle set and delete ops.

        Example sequence:
            # data_file contains:
            {"user:1": {"name": "Alice"}}

            # wal_file contains:
            {"operation": "set", "key": "user:2", "value": {"name": "Bob"}}
            {"operation": "delete", "key": "user:1"}

            # After recovery, self.data contains:
            {"user:2": {"name": "Bob"}}
        """
        try:
            # First load the last checkpoint
            if os.path.exists(self.data_file):
                with open(self.data_file, "rb") as f:
                    self.data = pickle.load(f)

            # Then replay any additional changes from WAL
            if os.path.exists(self.wal_file):
                with open(self.wal_file, "r") as f:
                    for line in f:
                        if line.strip():  # Skip empty lines
                            entry = json.loads(line)
                            if entry["operation"] == "set":
                                self.data[entry["key"]] = entry["value"]
                            elif entry["operation"] == "delete":
                                self.data.pop(entry["key"], None)
        except (IOError, json.JSONDecodeError, pickle.PickleError) as e:
            raise RuntimeError(f"Recovery failed: {e}")
    
    def set(self, key: str, value: Any):
        entry = WalEntry("set", key, value)
        self._append_wal(entry) # after this the (key, value) entry is durable on disk
        self.data[key] = value # we can update the in memory state now
    
    def delete(self, key: str):
        entry = WalEntry("delete", key, None)
        self._append_wal(entry)
        self.data.pop(key, None)
    
    def checkpoint(self):
        """ 
        Create a snapshot on persistent storage of current state 

            1. Current state: 
            data_file: {"user:1": {"name": "Alice"}}
            wal_file: [set user:2 {"name": "Bob"}]

            2. During checkpoint:
            data_file: {"user:1": {"name": "Alice"}}
            data_file.tmp: {"user:1": {"name": "Alice"}, "user:2": {"name": "Bob"}}
            wal_file: [set user:2 {"name": "Bob"}]

            3. After checkpoint:
            data_file: {"user:1": {"name": "Alice"}, "user:2": {"name": "Bob"}}
            wal_file: []  # Empty
        """
        temp_file = f"{self.data_file}.tmp"
        try:
            # write in-memory key value state to temp file
            with open(temp_file, "wb") as f:
                pickle.dump(self.data, f)
                f.flush()
                os.fsync(f.fileno())

            # atomically replace old checkpoint file
            os.rename(temp_file, self.data_file) # TODO shall we use rename or move?

            # clear wal
            if os.path.exists(self.wal_file):
                with open(self.wal_file, "r+") as f:
                    f.truncate(0)
                    f.flush()
                    os.fsync(f.fileno())
        except IOError as e:
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise RuntimeError(f"Checkpoint failed: {e}")