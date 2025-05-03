# Concurrency Memtable Freezing

How to handle concurrent reads and writes when memtable size threshold is exceeded?

## LevelDB

### concurrent write
1. size check
2. wait if already flushing
3. memtable switch

```cpp
// leveldb/db/db_impl.cc
Status DBImpl::MakeRoomForWrite(bool force) {

}
```

### concurrent reads

1. Atomic Flag for Immutable State `has_imm_.store(true, std::memory_order_release);`
2. Reference Counting for Safe Concurrent Access

### Atomic Flag has_imm_

![atomic flag](./atomic_flag.png)

States
1. Initial State
   1. only an active memtable exists.
   2. transition
      1. writer thread makes current memtable immutable.
      2. writer thread sets `has_imm_.store(true, memory_order_release)`
      3. writer thread creates a new active memtable.
      4. writer thread signals background thread.
2. Transition State: both active and immutable memtables exist. Read threads check both memtables.
3. Final State: immutable data now in SSTable, only memtable is active.

```mermaid
sequenceDiagram
    participant W as Writer Thread
    participant R1 as Reader Thread 1
    participant R2 as Reader Thread 2
    participant BG as Background Thread
    
    Note over W,BG: Initial State: Only Active Memtable
    
    rect rgb(240, 248, 255)
        Note over W: Memtable Full - Switching
        W->>W: imm_ = mem_
        W->>W: has_imm_.store(true, memory_order_release)
        W->>W: mem_ = new MemTable()
        W->>BG: Signal background thread
    end
    
    Note over W,BG: Transition State: Both Active & Immutable
    
    R1->>R1: has_imm_.load(memory_order_acquire)
    Note over R1: True - safe to access imm_
    R1->>R1: Check active memtable (not found)
    R1->>R1: Check immutable memtable (found)
    
    R2->>R2: has_imm_.load(memory_order_acquire)
    Note over R2: True - safe to access imm_
    R2->>R2: Check active memtable (found)
    
    BG->>BG: Flush immutable memtable to SSTable
    
    rect rgb(250, 240, 240)
        Note over BG: Flush Complete
        BG->>BG: imm_->Unref()
        BG->>BG: imm_ = nullptr
        BG->>BG: has_imm_.store(false, memory_order_release)
    end
    
    Note over W,BG: Final State: Only Active Memtable
    
    R1->>R1: has_imm_.load(memory_order_acquire)
    Note over R1: False - skip imm_ access
    R1->>R1: Check active memtable (not found)
    R1->>R1: Check SSTables
```