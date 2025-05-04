import bisect
from typing import List, Tuple, Any, Optional, Iterator

class MemTable:
    """
    We use a naive sorted list with binary search.
    Production databases like LevelDB and RocksDB typically use more sophisticated 
    data structures like Red-Black trees or Skip Lists.

    Example of memtable sorting:
        # Starting state:
        entries = [
            ("apple", 1),
            ("cherry", 2),
            ("zebra", 3)
        ]

        # Adding "banana" = 4:
        # 1. Find insertion point (between "apple" and "cherry")
        # 2. Insert new entry
        # 3. Result:
        entries = [
            ("apple", 1),
            ("banana", 4),
            ("cherry", 2),
            ("zebra", 3)
        ]
    """
    def __init__(self, max_size: int = 1000):
        self.entries: List[Tuple[str, Any]] = []
        self.max_size = max_size

    def add(self, key: str, value: Any):
        """Add or update a key-value pair"""
        idx = bisect.bisect_left([k for k, _ in self.entries], key)
        if idx < len(self.entries) and self.entries[idx][0] == key:
            self.entries[idx] = (key, value)
        else:
            self.entries.insert(idx, (key, value))

    def get(self, key: str) -> Optional[Any]:
        """Get value for key"""
        idx = bisect.bisect_left([k for k, _ in self.entries], key)
        if idx < len(self.entries) and self.entries[idx][0] == key:
            return self.entries[idx][1]
        return None

    def is_full(self) -> bool:
        """Check if memtable has reached max size"""
        return len(self.entries) >= self.max_size

    def range_scan(self, start_key: str, end_key: str) -> Iterator[Tuple[str, Any]]:
        """Scan entries within key range"""
        start_idx = bisect.bisect_left([k for k, _ in self.entries], start_key)
        end_idx = bisect.bisect_right([k for k, _ in self.entries], end_key)
        return iter(self.entries[start_idx:end_idx])
