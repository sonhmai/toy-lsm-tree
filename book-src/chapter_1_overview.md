# Chapter 1 Overview

## Databases landscape

![img_1.png](img_1.png)

In this book, we are going to write a row-based key-value database using log-structure storage engine. The engine will be implemented using Sorted String Tables (SSTables) and Log-Structured Merge (LSM) tree on object storage like AWS S3.

## LSM Tree Database

Three main components of a database storage engine built on LSM Tree are
1. memtable
2. sorted-string table
3. WAL (write-ahead log) file

`memtable` is an in-memory data structure that serves reads and writes. New writes go to the wal for persistence. The WAL is regularly sends to object storage for durability.

The words memtable and SSTable started from [the Google Bigtable paper](https://research.google/pubs/bigtable-a-distributed-storage-system-for-structured-data/)

```mermaid
classDiagram
    direction LR

    class DB {
        +Version currentVersion
        +MemTable activeMemTable
        +List~MemTable~ immutableMemTables
        +BlockCache blockCache
        +VersionSet versionSet
        +WriteAheadLog writeAheadLog
        +Get(key) Result
        +Put(key, value) Status
        +Delete(key) Status
        +NewIterator(options) Iterator
        #ScheduleFlush()
        #ScheduleCompaction()
    }

    class MemTable {
        +SkipList/BTree table
        +long approximateSize
        +Add(key, value, type)
        +Get(key) Result
        +NewIterator() Iterator
    }

    class SSTable {
        +File fileDescriptor
        +Key minKey
        +Key maxKey
        +int level
        +long tableSize
        +SSTableIndex indexBlock
        +BloomFilter filterBlock
        +Find(key) Result
        +NewIterator() Iterator
    }

    class SSTableIndex {
        +byte[] indexData
        +FindBlockOffset(key) long
    }

    class BloomFilter {
        +byte[] filterData
        +MayContain(key) bool
    }

    class BlockCache {
        +LRUCache~CacheKey, Block~ cache
        +long capacity
        +long usage
        +Lookup(cacheKey) Block
        +Insert(cacheKey, block) Status
    }

    class Version {
        +List~MemTable~ memTables
        +List~List~SSTable~~ sstablesPerLevel
        +int refCount
        +Get(key) Result
        +GetOverlappingSSTables(level, keyRange) List~SSTable~
    }

    class VersionSet {
        +Version currentVersion
        +File manifestFile
        +GetCurrentVersion() Version
        +LogAndApply(edit) Version
    }

    class Iterator {
        +MergingIterator mergedIterator
        +Version currentVersion
        +Valid() bool
        +Seek(key)
        +Next()
        +Key() Key
        +Value() Value
    }

    class WriteAheadLog {
        +File logFile
        +AddRecord(record) Status
        +Sync() Status
    }

    DB "1" *-- "1" VersionSet : manages
    DB "1" *-- "1" BlockCache : uses
    DB "1" *-- "1" WriteAheadLog : writes to
    DB "1" o-- "1" MemTable : active
    DB "1" o-- "0..*" MemTable : immutable

    VersionSet "1" o-- "1..*" Version : contains

    Version "1" o-- "0..*" MemTable : references immutable
    Version "1" o-- "0..*" SSTable : references by level

    SSTable "1" *-- "1" SSTableIndex : contains
    SSTable "1" *-- "1" BloomFilter : contains

    Iterator ..> Version : uses snapshot
    DB ..> Iterator : creates

    BlockCache ..> SSTable : caches blocks from


```

## The simplest storage engine

TODO
- implement the simple storage engine of book DDIA using append-only text file
- where are the weak spots
- how to address those weak spots by using SSTables and LSM tree storage engine


## References
- [Design Data Intensive Applications](https://www.amazon.com/Designing-Data-Intensive-Applications-Reliable-Maintainable/dp/1449373321)
- https://skyzh.github.io/mini-lsm


