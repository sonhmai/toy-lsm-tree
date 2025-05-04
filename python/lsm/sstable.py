import os
import pickle
from typing import Dict, Optional, Any, Iterator, Tuple

from lsm.memtable import MemTable

class SSTable:
    """
    When a memtable size exceeded our size threshold, it is marked as immutable and dumped
    to disk as SSTable.

    File layout:
    [size1][entry1][size2][entry2]...

    Example:
    [0x00000020][{"key": "apple", "value": 1}][0x00000024][{"key": "banana", "value": 4}]...
    """
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.index: Dict[str, int] = {}
        if os.path.exists(filename):
            self._load_index()

    def _load_index(self):
        """ load index from existing SSTable file """
        try:
            with open(self.filename, "rb") as f:
                f.seek(0)
                index_pos = int.from_bytes(f.read(8), "big")
                f.seek(index_pos)
                self.index = pickle.load(f)
        except (IOError, pickle.PickleError) as e:
            raise ValueError(f"Failed to load SSTable index: {e}")
        
    def write_memtable(self, memtable: MemTable):
        temp_file = f"{self.filename}.sstable"
        with open(temp_file, "wb") as f:
            # write index size for recovery
            index_pos = f.tell()
            f.write(b"\0" * 8) # placeholder for index pos
            # write data
            for key, value in memtable.entries:
                offset = f.tell()
                self.index[key] = offset
                entry_bytes = pickle.dumps((key, value))
                f.write(len(entry_bytes).to_bytes(4, "big"))
                f.write(entry_bytes)
            # write index at end
            index_offset = f.tell()
            pickle.dump(self.index, f)
            # update index position at start of file
            f.seek(index_pos)
            f.write(index_offset.to_bytes(8, "big"))
            f.flush()
            # os.sync(f.fileno()) ??
    
    def get(self, key: str) -> Optional[Any]:
        """ get value for key from SSTable """
        if key not in self.index:
            return None

        try:
            with open(self.filename, "rb") as f:
                f.seek(self.index[key])
                size = int.from_bytes(f.read(4), "big")
                entry = pickle.loads(f.read(size))
                return entry[1]
        except (IOError, pickle.PickleError) as e:
            raise ValueError(f"Failed to read from SSTable: {e}")

    def range_scan(self, start_key: str, end_key: str) -> Iterator[Tuple[str, Any]]:
        """ scan entries within key range """
        keys = sorted(k for k in self.index.keys() if start_key <= k <= end_key)
        for key in keys:
            value = self.get(key)
            if value is not None:
                yield (key, value)

