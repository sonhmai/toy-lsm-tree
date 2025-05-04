from typing import Dict, Any

from lsm.wal_entry import WalEntry

class WalStore:
    def __init__(self, data_file: str, wal_file: str) -> None:
        self.data_file = data_file # where we save temp data periodically during checkpoint like a db backup
        self.wal_file = wal_file # transaction log
        self.data: Dict[str, Any] = {}
        self._recover()

    def _append_wal(self, entry: WalEntry):
        raise NotImplementedError

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
        raise NotImplementedError
    
    def set(self, key: str, value: Any):
        raise NotImplementedError
    
    def delete(self, key: str):
        """ delete a key with WAL """
        raise NotImplementedError
    
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
