# Concurrency Memtable Freezing in LevelDB

- [Concurrency Memtable Freezing in LevelDB](#concurrency-memtable-freezing-in-leveldb)
  - [Threading Model](#threading-model)
  - [Writer Thread](#writer-thread)
  - [Background Thread](#background-thread)
    - [Background Memtable Flushing Details](#background-memtable-flushing-details)
  - [Reader Thread](#reader-thread)
    - [Atomic Flag has\_imm\_](#atomic-flag-has_imm_)

How to handle concurrent reads and writes when memtable size threshold is exceeded?
1. Old memtable is moved to immutable memtable list (imm).
2. A new memtable is installed atomically.
3. Background flushes are scheduled without blocking.

## Threading Model

```mermaid
flowchart TD
    %% Define swimlanes
    subgraph Writer_Thread ["Writer Thread"]
        w_start([Write Request])
        w_check_size{Memtable size > threshold?}
        w_continue[Continue with write to active memtable]
        w_switch_mem[Switch memtable]
        w_set_flag[Set has_imm_=true with memory_order_release]
        w_new_mem[Create new active memtable]
        w_signal[Signal background thread]
        w_end([Write completed])
    end

    subgraph Read_Thread_1 ["Read Thread 1 (During Transition)"]
        r1_start([Read Request])
        r1_load_flag[Load has_imm_ with memory_order_acquire]
        r1_check_flag{has_imm_ == true?}
        r1_check_active[Check active memtable]
        r1_found_active{Found inactive?}
        r1_check_imm[Check immutable memtable]
        r1_found_imm{Found in immutable?}
        r1_check_sst[Check SSTables]
        r1_end([Return result])
    end

    subgraph Read_Thread_2 ["Read Thread 2 (After Flush)"]
        r2_start([Read Request])
        r2_load_flag[Load has_imm_ with memory_order_acquire]
        r2_check_flag{has_imm_ == true?}
        r2_check_active[Check active memtable]
        r2_found_active{Found in active?}
        r2_check_sst[Check SSTables]
        r2_end([Return result])
    end

    subgraph Background_Thread ["Background Thread"]
        bg_wait[Wait for signal]
        bg_start_flush[Start flush of immutable memtable]
        bg_write_sst[Write SSTable to disk]
        bg_update_meta[Update metadata]
        bg_unref[Unref immutable memtable]
        bg_null[Set imm_ = nullptr]
        bg_clear_flag[Set has_imm_=false with memory_order_release]
        bg_end([Flush completed])
    end

    %% Connect Writer Thread
    w_start --> w_check_size
    w_check_size -->|No| w_continue --> w_end
    w_check_size -->|Yes| w_switch_mem --> w_set_flag
    w_set_flag --> w_new_mem --> w_signal --> w_continue

    %% Connect Background Thread
    w_signal -.-> bg_wait
    bg_wait --> bg_start_flush --> bg_write_sst --> bg_update_meta
    bg_update_meta --> bg_unref --> bg_null --> bg_clear_flag --> bg_end

    %% Connect Read Thread 1 (during transition)
    r1_start --> r1_load_flag --> r1_check_flag
    r1_check_flag -->|Yes| r1_check_active
    r1_check_flag -->|No| r1_check_active
    r1_check_active --> r1_found_active
    r1_found_active -->|Yes| r1_end
    r1_found_active -->|No| r1_check_imm
    r1_check_imm --> r1_found_imm
    r1_found_imm -->|Yes| r1_end
    r1_found_imm -->|No| r1_check_sst --> r1_end

    %% Connect Read Thread 2 (after flush completed)
    r2_start --> r2_load_flag --> r2_check_flag
    r2_check_flag -->|Yes| r2_check_active
    r2_check_flag -->|No| r2_check_active
    r2_check_active --> r2_found_active
    r2_found_active -->|Yes| r2_end
    r2_found_active -->|No| r2_check_sst --> r2_end

    %% Create synchronization points with atomic flag
    w_set_flag -.-> r1_load_flag
    bg_clear_flag -.-> r2_load_flag

    %% Define styles
    classDef process fill:#d4f1f9,stroke:#333,stroke-width:1px
    classDef decision fill:#ffdebd,stroke:#333,stroke-width:1px
    classDef terminal fill:#e1e1e1,stroke:#333,stroke-width:1px
    classDef atomic fill:#d8f8d8,stroke:#333,stroke-width:2px

    class w_continue,w_switch_mem,w_new_mem,r1_check_active,r1_check_imm,r1_check_sst,r2_check_active,r2_check_sst,bg_start_flush,bg_write_sst,bg_update_meta,bg_unref,bg_null process
    class w_check_size,r1_check_flag,r1_found_active,r1_found_imm,r2_check_flag,r2_found_active decision
    class w_start,w_end,r1_start,r1_end,r2_start,r2_end,bg_wait,bg_end terminal
    class w_set_flag,r1_load_flag,r2_load_flag,bg_clear_flag atomic
```

## Writer Thread

1. size check
2. wait if already flushing
3. memtable switch

```cpp

```

**Memtable Switch**
1. new WAL log file created
2. current memtable moved to `imm_` immutable memtable
3. `has_imm_` flag is atomically set using memory order release semantics
4. new empty memtable is created
5. background compaction is scheduled

```cpp
// leveldb/db/db_impl.cc
Status DBImpl::MakeRoomForWrite(bool force) {
    // step 1 create new WAL log file
    assert(versions_->PrevLogNumber() == 0);
    uint64_t new_log_number = versions_->NewFileNumber();
    WritableFile* lfile = nullptr;
    s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
    // ... [create new log file]
    delete log_;
    delete logfile_;
    logfile_ = lfile;
    logfilenumber_ = new_log_number;
    log_ = new log::Writer(lfile);

    // step 2
    imm_ = mem_;
    // step 3
    has_imm_.store(true, std::memory_order_release);

    // step4: create new empty memtable
    mem_ = new Memtable(internal_comparator_);
    mem_->Ref();

    // step5: signal for background thread to flush immutable memtable to SSTable
    MaybeScheduleCompaction();
    // ...
}

// signal the background thread to do work async
void DBImpl::MaybeScheduleCompaction() {
    mutex_.AssertHeld();
    if (!background_compaction_scheduled_ && !shutting_down_.load() && 
      bg_error_.ok() && (imm_ != nullptr || versions_->NeedsCompaction())) {
        background_compaction_scheduled_ = true;
        env_->Schedule(&DBImpl:BGWork, this); // schedule work for background thread
    }
```

FAQ
- why we need to create new log file in writer thread?
  - **simplify design and storage management**: 
    - it makes the design simple as one log file matches to one memtable.
    - preventing any log file from growing too large.
    - we can just delete the log file of the flushed memtable (to SSTable) without any impact.
    - without separate log file, it's hard to know which part of log file to be deleted.
  - **simplify recovery:**
    - each log file matches to a memtable that might not have been flushed.
    - system can easily find out which log file to be replayed for current memtable.

## Background Thread

Writer thread signals the background thread to flush memtable to SSTable asynchronously.
Background thread executes `CompactMemTable` method.

Background thread actions
1. wake up and acquire mutex
2. do compaction work of flushing immutable memtable to SSTable
   1. flush immutable memtable to disk
   2. update version metadata
   3. clear immutable memtable pointer
   4. clear atomic flag
3. signal completion to any waiting threads after finished

Shared data structures with other reader, writer threads
1. immutable memtable pointer `imm_`
2. version metadata for the database

### Background Memtable Flushing Details
1. write immutable memtable to disk as a Level-0 SSTable
2. update version metadata to include new SSTable

```cpp
Status DBImpl::CompactMemTable() {
    mutex_.AssertHeld();
    assert(imm_ != nullptr);

    // save memtable as L0 SSTable

    VersionEdit edit;
    edit.SetLogNumber(logfile_number_);
    edit.AddFile()
    
}
```

Questions
- what is version metadata? 
- why do we need version metadata?
  - consistency: readers operate on specific versions while the database changes (writes, compaction).
- why do we change it here background flushing?

Edit version metadata in background thread compaction
```mermaid
sequenceDiagram
    participant BG as Background Thread
    participant VS as VersionSet
    participant V1 as Version (Old)
    participant V2 as Version (Current)
    participant LE as Log & Edit
    participant FS as File System

    Note over BG,FS: During Memtable Flush
    
    BG->>FS: Write immutable memtable to SSTable
    FS-->>BG: Return file number & size
    
    BG->>LE: Create VersionEdit
    Note right of LE: AddFile(0, file_number, size, smallest, largest)
    Note right of LE: SetLogNumber(log_number)
    
    BG->>VS: LogAndApply(edit)
    
    VS->>LE: Log edit to MANIFEST
    Note right of LE: Serialized format for recovery
    LE->>FS: Write to MANIFEST file
    
    VS->>V1: Take reference to prevent deletion
    VS->>V1: Apply edit to create new version
    V1-->>VS: Return new Version object
    
    VS->>V2: Install as current_
    Note right of V2: All new readers will use this version
    
    VS->>V1: Unref (may delete if no readers)
    
    Note over BG,FS: Version Transition Complete
```

## Reader Thread

1. Atomic Flag for Immutable State `has_imm_.store(true, std::memory_order_release);`
2. Reference Counting for Safe Concurrent Access

### Atomic Flag has_imm_

![atomic flag](./atomic_flag.png)

States
1. **Initial State**
   1. only an active memtable exists.
   2. transition
      1. writer thread makes current memtable immutable.
      2. writer thread sets `has_imm_.store(true, memory_order_release)`
      3. writer thread creates a new active memtable.
      4. writer thread signals background thread.
2. **Transition State** 
   1. both active and immutable memtables exist. 
   2. read threads check both memtables active and immutable.
   3. transition
      1. background thread (BT) finishes flushing to SSTable.
      2. BT releases immutable memtable.
      3. Future readers will skip checking now non-existing immutable memtable.
3. **Final State**
   1. immutable data now in SSTable, only memtable is active.

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
