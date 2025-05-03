# Concurrency Memtable Freezing in LevelDB

How to handle concurrent reads and writes when memtable size threshold is exceeded?

## Threading Model

```mermaid
flowchart TD
    %% Define swimlanes
    subgraph Writer_Thread ["Writer Thread"]
        w_start([Write Request])
        w_check_size{Memtable size\n> threshold?}
        w_continue[Continue with write\nto active memtable]
        w_switch_mem[Switch memtable]
        w_set_flag[Set has_imm_=true\nwith memory_order_release]
        w_new_mem[Create new active memtable]
        w_signal[Signal background thread]
        w_end([Write completed])
    end

    subgraph Read_Thread_1 ["Read Thread 1 (During Transition)"]
        r1_start([Read Request])
        r1_load_flag[Load has_imm_\nwith memory_order_acquire]
        r1_check_flag{has_imm_\n== true?}
        r1_check_active[Check active memtable]
        r1_found_active{Found in\nactive?}
        r1_check_imm[Check immutable memtable]
        r1_found_imm{Found in\nimmutable?}
        r1_check_sst[Check SSTables]
        r1_end([Return result])
    end

    subgraph Read_Thread_2 ["Read Thread 2 (After Flush)"]
        r2_start([Read Request])
        r2_load_flag[Load has_imm_\nwith memory_order_acquire]
        r2_check_flag{has_imm_\n== true?}
        r2_check_active[Check active memtable]
        r2_found_active{Found in\nactive?}
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
        bg_clear_flag[Set has_imm_=false\nwith memory_order_release]
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