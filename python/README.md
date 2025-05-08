# LSM Tree in Python

```
# Install uv if not existing
curl -Ls https://astral.sh/uv/install.sh | bash

uv run pytest
# run a specific test
uv run pytest -sv -k test_range_scan

uv run main.py
```

## Design

Components
- Write-Ahead Logging (WAL) for durability and crash recovery
- MemTable for fast in-memory operations with sorted data
- Sorted String Tables (SSTables) for efficient disk storage
- Log-Structured Merge (LSM) Tree to tie everything together

![lsm](./lsm.png)

### Atomicity

## Limitations
Storage and Performance:
- Simple list of SSTables instead of a leveled structure.
- Inefficient compaction that merges all tables at once.
- No way to skip unnecessary SSTable reads; scanning all tables for a query is inefficient.


Concurrency:
- Basic locking that locks the entire operation
- No support for transactions across multiple operations
- The compaction process blocks all other operations

## References
- Apache Cassandra Architecture — A real-world implementation of LSM trees, great for understanding how these concepts scale
- LevelDB Documentation — Google’s key-value store, excellent for understanding practical optimizations
- RocksDB Compaction — Deep dive into advanced compaction strategies
- Database Internals — A comprehensive book on database system design
- https://hackernoon.com/how-to-build-a-database-from-scratch-understanding-lsm-trees-and-storage-engines-part-1