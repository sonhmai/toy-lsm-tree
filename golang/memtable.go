package main

import (
	"sync"
)

// valueType indicates whether an entry represents a PUT or a DELETE (tombstone).
type valueType byte

const (
	typePut    valueType = 1
	typeDelete valueType = 2
)

// internalKV holds the value and its type (Put or Delete).
// This allows us to store tombstones in the map.
type internalKV struct {
	value []byte
	vtype valueType
}

// MemTable is an in-memory buffer for recent writes.
// NOTE: This simple implementation uses a standard Go map and does NOT
// maintain key order, which is a requirement for efficient flushing and
// range scans in a real LSM Tree. A production implementation would
// typically use an ordered data structure like a Skip List, BTree, Red-Black Tree, etc.
type MemTable struct {
	mu            sync.RWMutex
	kv            map[string]internalKV
	estimatedSize int64 // Approximate size in bytes
}

func NewMemTable() *MemTable {
	return &MemTable{
		kv:            make(map[string]internalKV),
		estimatedSize: 0,
	}
}

// Put inserts or updates a key-value pair.
func (mt *MemTable) Put(key string, value []byte) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Check if key exists to calculate size difference accurately
	existing, exists := mt.kv[key]
	var oldSize int64
	if exists {
		oldSize = int64(len(key) + len(existing.value)) // Approximate existing size
	}

	newValue := internalKV{
		value: value,
		vtype: typePut,
	}
	mt.kv[key] = newValue

	// Update estimated size
	newSize := int64(len(key) + len(value)) // Approximate new size
	mt.estimatedSize += (newSize - oldSize)

	return nil // In this simple version, Put always succeeds
}

// Delete marks a key as deleted (writes a tombstone).
func (mt *MemTable) Delete(key string) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Check if key exists to calculate size difference accurately
	existing, exists := mt.kv[key]
	var oldSize int64
	if exists {
		// If it was already a tombstone, size calculation is different,
		// but for simplicity, we'll approximate based on previous value size.
		oldSize = int64(len(key) + len(existing.value))
	}

	// Tombstone has nil value but typeDelete
	tombstone := internalKV{
		value: nil,
		vtype: typeDelete,
	}
	mt.kv[key] = tombstone

	// Update estimated size - Tombstones still take up space (key + marker)
	// We approximate tombstone value size as 0 here.
	newSize := int64(len(key))
	mt.estimatedSize += (newSize - oldSize)

	return nil // In this simple version, Delete always succeeds
}

// Get retrieves the value for a key.
// It returns the value, a boolean indicating if the key was found (and not deleted),
// and an error (which is always nil in this simple version).
func (mt *MemTable) Get(key string) ([]byte, bool, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	internalVal, exists := mt.kv[key]
	if !exists {
		return nil, false, nil // Not found
	}

	if internalVal.vtype == typeDelete {
		return nil, false, nil // Found, but it's a tombstone (deleted)
	}

	// Found a regular PUT value
	// Return a copy to prevent modification of internal slice
	valueCopy := make([]byte, len(internalVal.value))
	copy(valueCopy, internalVal.value)
	return valueCopy, true, nil
}

// Size returns the approximate size of the MemTable in bytes.
func (mt *MemTable) Size() int64 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	// Return a copy of the size for safety, although int64 is often atomic on 64-bit
	size := mt.estimatedSize
	return size
}

// Len returns the number of entries (including tombstones) in the MemTable.
// Useful for testing or simple metrics.
func (mt *MemTable) Len() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return len(mt.kv)
}
