import datetime
import json
from typing import Any

class WalEntry:
    def __init__(self, operation: str, key: str, value: Any) -> None:
        self.timestamp = datetime.utcnow().isoformat()
        self.operation = operation # set or delete
        self.key = key
        self.value = value

    def serialize(self) -> str:
        """
        Serialized wal entry to json for readability. 
        Being less efficient than binary format is ok.

        Examples
        {
            "timestamp": "2024-03-20T14:30:15.123456",
            "operation": "set",
            "key": "user:1",
            "value": {"name": "Alice"}
        }
        """
        return json.dumps({
            'timestamp': self.timestamp, # "2024-03-20T14:30:15.123456" ISO format: readable and sortable
            'operation': self.operation,
            'key': self.key,
            'value': self.value,
        })