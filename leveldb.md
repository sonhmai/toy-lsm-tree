# LevelDB

## MemTable APIs

Based on `leveldb/memtable.h` and its usage:

*   **Constructor (`MemTable(const InternalKeyComparator& comparator)`)**
    *   Creates a new, empty MemTable.
    *   Requires a comparator that understands LevelDB's internal key format (user key + sequence number + value type) to maintain the correct sorted order within the underlying Skip List.

*   **`Ref()` / `Unref()`**
    *   Standard reference counting methods used for managing the MemTable's lifetime, especially when it becomes immutable.
    *   `Ref()` increments the reference count.
    *   `Unref()` decrements the reference count. When the count reaches zero, the `MemTable` object and its associated memory (`Arena`) are typically deleted.
    *   Allows safe sharing of immutable MemTables with concurrent readers and background flush operations.

*   **`ApproximateMemoryUsage()`**
    *   Returns an estimate (in bytes) of the memory currently consumed by the MemTable.
    *   This includes the data stored and overhead from the underlying Skip List and `Arena` allocator.
    *   Used by the DB implementation to check if the MemTable has reached its configured size threshold (`write_buffer_size`) and needs to be flushed.

*   **`Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value)`**
    *   The primary method for inserting entries (Puts or Deletes) into the MemTable.
    *   Takes the globally assigned `SequenceNumber` for the operation, the `ValueType` (`kTypeValue` for Put, `kTypeDeletion` for Delete/Tombstone), the user `key`, and the `value`.
    *   Internally constructs the full "Internal Key" (encoding key, sequence, and type) and inserts it along with the value into the Skip List.

*   **`Get(const LookupKey& key, std::string* value, Status* s)`**
    *   Retrieves the most recent entry for a given key *at or before* a specific sequence number (snapshot).
    *   The `LookupKey` bundles the user key and the target sequence number.
    *   Searches the Skip List using the internal comparator.
    *   If an entry for the key is found with a sequence number less than or equal to the lookup sequence:
        *   If the entry's type is `kTypeValue`, the `value` is copied into the output string, and the status `s` is set to `Ok`.
        *   If the entry's type is `kTypeDeletion`, the status `s` is set to `NotFound`.
    *   If no such entry is found (either key doesn't exist or only exists with higher sequence numbers), the status `s` is set to `NotFound`.

*   **`NewIterator()`**
    *   Creates and returns a new `Iterator` object.
    *   This iterator allows traversal of all entries within *this specific* MemTable in sorted order (as defined by the `InternalKeyComparator`).
    *   Essential for merging results from multiple sources (MemTables, SSTables) during reads and for writing sorted data to SSTables during flushes.