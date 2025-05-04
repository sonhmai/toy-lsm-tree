package main

import (
	"bytes"
	"sync"
	"testing"
)

func checkGet(t *testing.T, mt *MemTable, key string, expectedValue []byte, expectedFound bool) {
	t.Helper()
	value, found, err := mt.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) unexpected error: %v", key, err)
	}
	if found != expectedFound {
		t.Errorf("Get(%q) found mismatch: got %v, want %v", key, found, expectedFound)
	}
	if !bytes.Equal(value, expectedValue) {
		t.Errorf("Get(%q) value mismatch: got %q, want %q", key, value, expectedValue)
	}
}

func TestMemTable_PutGet(t *testing.T) {
	mt := NewMemTable()

	// Test basic Put and Get
	key1 := "key1"
	val1 := []byte("value1")
	err := mt.Put(key1, val1)
	if err != nil {
		t.Fatalf("Put(%q) unexpected error: %v", key1, err)
	}
	checkGet(t, mt, key1, val1, true)

	// Test Get non-existent key
	checkGet(t, mt, "nonexistent", nil, false)
}

func TestMemTable_Overwrite(t *testing.T) {
	mt := NewMemTable()
	key := "key-overwrite"
	val1 := []byte("value-old")
	val2 := []byte("value-new")

	mt.Put(key, val1)
	checkGet(t, mt, key, val1, true)

	mt.Put(key, val2)                // Overwrite
	checkGet(t, mt, key, val2, true) // Should get the new value
}

func TestMemTable_Delete(t *testing.T) {
	mt := NewMemTable()
	key := "key-delete"
	val := []byte("value-to-delete")

	// Put then Delete
	mt.Put(key, val)
	checkGet(t, mt, key, val, true)
	lenBeforeDelete := mt.Len()

	err := mt.Delete(key)
	if err != nil {
		t.Fatalf("Delete(%q) unexpected error: %v", key, err)
	}

	// Get after delete should return not found
	checkGet(t, mt, key, nil, false)

	// Check that the entry (tombstone) still exists internally
	lenAfterDelete := mt.Len()
	if lenAfterDelete != lenBeforeDelete {
		t.Errorf("Len() after delete mismatch: got %d, want %d (tombstone should exist)", lenAfterDelete, lenBeforeDelete)
	}

	// Test Delete non-existent key
	err = mt.Delete("nonexistent-delete")
	if err != nil {
		t.Fatalf("Delete(nonexistent) unexpected error: %v", err)
	}
	checkGet(t, mt, "nonexistent-delete", nil, false)
}

func TestMemTable_PutAfterDelete(t *testing.T) {
	mt := NewMemTable()
	key := "key-put-delete-put"
	val1 := []byte("value-first")
	val2 := []byte("value-second")

	mt.Put(key, val1)
	mt.Delete(key)
	checkGet(t, mt, key, nil, false) // Verify deleted

	mt.Put(key, val2)                // Put again after delete
	checkGet(t, mt, key, val2, true) // Should get the new value
}

func TestMemTable_Size(t *testing.T) {
	mt := NewMemTable()

	if mt.Size() != 0 {
		t.Errorf("Initial size mismatch: got %d, want 0", mt.Size())
	}

	key1 := "sizekey1"
	val1 := []byte("sizevalue1")
	mt.Put(key1, val1)
	size1 := mt.Size()
	if size1 <= 0 {
		t.Errorf("Size after first Put should be > 0, got %d", size1)
	}
	expectedSize1 := int64(len(key1) + len(val1))
	if size1 != expectedSize1 {
		t.Logf("Size after first Put: got %d, approx expected %d (exact match not required)", size1, expectedSize1)
	}

	key2 := "sizekey2"
	val2 := []byte("sv2")
	mt.Put(key2, val2)
	size2 := mt.Size()
	if size2 <= size1 {
		t.Errorf("Size after second Put should be > size1, got %d, want > %d", size2, size1)
	}

	// Delete first key - size should decrease because len(val1) > 0
	mt.Delete(key1)
	size3 := mt.Size()
	if size3 == size2 {
		t.Errorf("Size after delete should have changed from %d, but got %d", size2, size3)
	}
	expectedSize3 := int64(len(key1)) + int64(len(key2)+len(val2)) // key1 (tombstone) + key2 (value)
	if size3 != expectedSize3 {
		t.Logf("Size after delete: got %d, approx expected %d (exact match not required)", size3, expectedSize3)
	}

	// Overwrite second key with same size value - size should ideally not change
	mt.Put(key2, []byte("SV2")) // Same length value
	size4 := mt.Size()
	if size4 != size3 {
		t.Errorf("Size after overwrite with same length value should not change: got %d, want %d", size4, size3)
	}

	// Overwrite second key with different size value - size should change
	mt.Put(key2, []byte("Different size value"))
	size5 := mt.Size()
	if size5 == size4 {
		t.Errorf("Size after overwrite with different length value should change: got %d, want != %d", size5, size4)
	}
}

// Basic check to ensure locks prevent data races. Run with go test -race flag
func TestMemTable_Concurrency(t *testing.T) {
	mt := NewMemTable()
	key := "concurrent_key"
	val := []byte("concurrent_value")

	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrent Puts/Deletes
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			if i%4 == 0 {
				mt.Put(key, val)
			} else if i%4 == 1 {
				mt.Delete(key)
			} else if i%4 == 2 {
				mt.Get(key)
			} else {
				mt.Size()
			}
		}(i)
	}

	// Concurrent Get/Size
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			mt.Get(key) // read lock
			mt.Size()   // atomic load
		}()
	}

	wg.Wait()
	// No explicit checks here, the main goal is to run with `go test -race`
	// to detect race conditions, which shouldn't occur if locks are correct.
	t.Log("Concurrency test finished. Run with 'go test -race' to check for races.")
}
