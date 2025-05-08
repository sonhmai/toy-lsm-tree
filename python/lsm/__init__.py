import os
from pathlib import Path
from threading import RLock
from typing import List, Any

from .memtable import MemTable
from .sstable import SSTable
from .wal_store import WalStore

class DatabaseError(Exception):
    pass

class LSMTree:
    """
    
    Writing data
        # Starting state:
        memtable: empty
        sstables: []

        # After db.set("user:1", {"name": "Alice"})
        memtable: [("user:1", {"name": "Alice"})]
        sstables: []

        # After 1000 more sets (memtable full)...
        memtable: empty
        sstables: [sstable_0.db]  # Contains sorted data

        # After 1000 more sets...
        memtable: empty
        sstables: [sstable_0.db, sstable_1.db]

    Reading
        # Database state:
        memtable: [("user:3", {"name": "Charlie"})]
        sstables: [
            sstable_0.db: [("user:1", {"name": "Alice"})],
            sstable_1.db: [("user:2", {"name": "Bob"})]
        ]

        # Reading "user:3" -> Finds it in memtable
        # Reading "user:1" -> Checks memtable, then finds in sstable_0.db
        # Reading "user:4" -> Checks everywhere, returns None

    Lock to prevent muli threads data races. Example of why we need locks:
        # Without locks:
        Thread 1: reads data["x"] = 5
        Thread 2: reads data["x"] = 5
        Thread 1: writes data["x"] = 6
        Thread 2: writes data["x"] = 7  # Last write wins, first update lost!

        # With locks:
        Thread 1: acquires lock
        Thread 1: reads data["x"] = 5
        Thread 2: acquires lock, wait as Thread 1 has it.
        Thread 1: writes data["x"] = 6
        Thread 1: releases lock
        Thread 2: has lock now
        Thread 2: reads data["x"] = 6
        Thread 2: writes data["x"] = 7
        Thread 2: releases lock
    """
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        try:
            # Check if path exists and is a file
            if self.base_path.exists() and self.base_path.is_file():
                raise DatabaseError(f"Cannot create database: '{base_path}' is a file")

            self.base_path.mkdir(parents=True, exist_ok=True)

        except (OSError, FileExistsError) as e:
            raise DatabaseError(
                f"Failed to initialize database at '{base_path}': {str(e)}"
            )

        # Our "Inbox" for new data
        self.memtable = MemTable(max_size=1000)

        # Our "Folders" of sorted data
        self.sstables: List[SSTable] = []

        self.max_sstables = 5  # Limit on number of SSTables
        self.lock = RLock()

        self.wal = WalStore(
            str(self.base_path / "data.db"), str(self.base_path / "wal.log")
        )

        self._load_sstables()
        if len(self.sstables) > self.max_sstables:
            self._compact()

    def _load_sstables(self):
        """Load existing SSTables from disk, having their indices in memory to check whether a key exists"""
        self.sstables.clear()
        for file in sorted(self.base_path.glob("sstable_*.db")):
            self.sstables.append(SSTable(str(file)))

    def set(self, key: str, value: Any):
        """Set a key-value pair"""
        with self.lock:
            if not isinstance(key, str):
                raise ValueError("Key must be a string")
            # write to wal first, then memtable, flush to sstable if memtable is full
            self.wal.set(key, value)
            self.memtable.add(key, value)
            if self.memtable.is_full():
                self._flush_memtable()

    def _flush_memtable(self):
        """Flush memtable to disk as new SSTable"""
        if not self.memtable.entries:
            return  # Skip if empty

        # Create new SSTable with a unique name
        sstable = SSTable(str(self.base_path / f"sstable_{len(self.sstables)}.db"))
        sstable.write_memtable(self.memtable)
        # Add to our list of SSTables
        self.sstables.append(sstable)

        # Create fresh memory table
        self.memtable = MemTable()

        # Create a checkpoint in WAL
        self.wal.checkpoint()

        # Compact if we have too many SSTables
        if len(self.sstables) > self.max_sstables:
            self._compact()

    def _compact(self):
        """Merge multiple SSTables on disk into one
        
            # Before compaction:
            sstables: [
                sstable_0.db: [("apple", 1), ("banana", 2)],
                sstable_1.db: [("banana", 3), ("cherry", 4)],
                sstable_2.db: [("apple", 5), ("date", 6)]
            ]

            # After compaction:
            sstables: [
                sstable_compacted.db: [
                    ("apple", 5),    # Latest value wins
                    ("banana", 3),   # Latest value wins
                    ("cherry", 4),
                    ("date", 6)
                ]
            ]
        """
        try:
            # Create merged memtable
            merged = MemTable(max_size=float("inf"))

            # Merge all SSTables
            for sstable in self.sstables:
                for key, value in sstable.range_scan("", "~"):  # Full range
                    merged.add(key, value)

            # Write merged data to new SSTable
            new_sstable = SSTable(str(self.base_path / "sstable_compacted.db"))
            new_sstable.write_memtable(merged)

            # Remove old SSTables
            old_files = [sst.filename for sst in self.sstables]
            self.sstables = [new_sstable]

            # Delete old files
            for file in old_files:
                try:
                    os.remove(file)
                except OSError:
                    pass  # Ignore deletion errors

        except Exception as e:
            raise DatabaseError(f"Compaction failed: {e}") 
        
    def delete(self, key: str):
        """Delete a key"""
        with self.lock:
            self.wal.delete(key)
            self.set(key, None)  # Use None as tombstone

    def close(self):
        """Ensure all data is persisted to disk"""
        with self.lock:
            if self.memtable.entries:  # If there's data in memtable
                self._flush_memtable()
            self.wal.checkpoint()  # Ensure WAL is up-to-date
