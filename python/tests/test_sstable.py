import pytest
import os
import tempfile
import shutil

from lsm.sstable import SSTable
from lsm.memtable import MemTable

@pytest.fixture
def temp_dir():
    # Create a temporary directory for test files
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Clean up test directory
    shutil.rmtree(temp_dir)

@pytest.fixture
def sstable_path(temp_dir):
    dir_from_env = os.getenv("SS_TABLE_PATH")
    test_dir = dir_from_env if dir_from_env else temp_dir
    print(f"\nSSTable dir: {test_dir}")
    return os.path.join(test_dir, "test.sstable")

def test_create_empty_sstable(temp_dir):
    # index should be empty. sstable can be created.
    sstable_path = os.path.join(temp_dir, "test.sstable")
    sstable = SSTable(sstable_path)
    assert sstable.filename == sstable_path
    assert sstable.index == {}

def test_memtable_write_and_then_read(sstable_path):
    """Test writing basic memtable data to SSTable"""
    # Create memtable with test data
    memtable = MemTable(max_size=1000)
    memtable.add("key1", "value1")
    memtable.add("key2", "value2")
    memtable.add("key3", "value3")
    
    # Write to SSTable
    sstable = SSTable(sstable_path)
    sstable.write_memtable(memtable)
    
    # Verify index was built correctly
    assert len(sstable.index) == 3
    assert "key1" in sstable.index
    assert "key2" in sstable.index
    assert "key3" in sstable.index

    # Create new SSTable instance to test loading
    loaded_sstable = SSTable(sstable_path)
    
    # Test getting values
    assert loaded_sstable.get("key1") == "value1"
    assert loaded_sstable.get("key2") == "value2"
    assert loaded_sstable.get("key3") == "value3"
    assert loaded_sstable.get("key4") is None # not existing

def test_range_scan(sstable_path):
    memtable = MemTable(max_size=1000)
    memtable.add("apple", 1)
    memtable.add("cherry", 3)
    # banana added after cherry. should be sorted (come before cherry) in sstable
    memtable.add("banana", 2) 
    memtable.add("date", 4)
    memtable.add("elderberry", 5)
    
    # Write to SSTable
    sstable = SSTable(sstable_path)
    sstable.write_memtable(memtable)
    
    # Range scan should cover all range, keys should be sorted
    results = list(sstable.range_scan("banana", "date"))
    assert len(results) == 3
    assert results[0] == ("banana", 2)
    assert results[1] == ("cherry", 3)
    assert results[2] == ("date", 4)
    
    # Test edge case: query range is bigger than keys range
    results = list(sstable.range_scan("a", "z"))
    assert len(results) == 5
    assert results[1] == ("banana", 2) # banana and cherry should be sorted
    
    # range scan not overlapping with keys should return nothing
    results = list(sstable.range_scan("z", "zz"))
    assert len(results) == 0

def test_empty_table_range_scan(sstable_path):
    sstable = SSTable(sstable_path)
    results = list(sstable.range_scan("a", "z"))
    assert len(results) == 0

def test_file_format_compatibility(sstable_path):
    """Test file format by manually checking written file"""
    memtable = MemTable(max_size=1000)
    memtable.add("key", "value")
    
    sstable = SSTable(sstable_path)
    sstable.write_memtable(memtable)
    
    # temp file should not exist after write because it was renamed into sstable
    temp_file = f"{sstable_path}.temp"
    assert not os.path.exists(temp_file)
    assert os.path.exists(sstable_path)

# def test_error_handling_invalid_file(sstable_path):
#     """Test error handling for corrupted files"""
#     # Create corrupt file
#     with open(sstable_path, "wb") as f:
#         f.write(b"INVALID_DATA")
        
#     # Test that loading fails gracefully
#     with pytest.raises(ValueError):
#         SSTable(sstable_path)

def test_complex_data_types(sstable_path):
    memtable = MemTable(max_size=1000)
    memtable.add("list_key", [1, 2, 3, 4])
    memtable.add("dict_key", {"nested": {"value": 42}})
    memtable.add("tuple_key", (1, 2, 3))
    
    sstable = SSTable(sstable_path)
    sstable.write_memtable(memtable)
    
    # Verify complex data can be stored and retrieved
    assert sstable.get("list_key") == [1, 2, 3, 4]
    assert sstable.get("dict_key") == {"nested": {"value": 42}}
    assert sstable.get("tuple_key") == (1, 2, 3)

def test_large_dataset(sstable_path):
    memtable = MemTable(max_size=10000)
    
    # Add 100 entries
    for i in range(10000):
        key = f"key_{i:03d}"
        value = f"value_{i}"
        memtable.add(key, value)
    
    sstable = SSTable(sstable_path)
    sstable.write_memtable(memtable)
    
    # Verify index size
    assert len(sstable.index) == 10000
    
    # Verify random access
    assert sstable.get("key_050") == "value_50"
    
    # Verify range scan
    results = list(sstable.range_scan("key_010", "key_020"))
    assert len(results) == 11  # includes both boundaries

def test_memtable_should_maintain_keys_ordering(sstable_path):
    memtable = MemTable(max_size=100)
    
    # Add keys in random order
    keys = ["charlie", "beta", "alpha", "delta"]
    for i, key in enumerate(keys):
        memtable.add(key, i)
    
    # Verify keys are sorted in memtable
    entries = list(memtable.entries)
    assert entries[0][0] == "alpha"
    assert entries[1][0] == "beta"
    assert entries[2][0] == "charlie"
    assert entries[3][0] == "delta"
    
    sstable = SSTable(sstable_path)
    sstable.write_memtable(memtable)
    
    # Verify SSTable preserves ordering by reading entries in sequence
    # We'll use range_scan with a wide range to get all entries in order
    results = list(sstable.range_scan("a", "z"))
    assert len(results) == 4
    assert results[0][0] == "alpha"
    assert results[1][0] == "beta"
    assert results[2][0] == "charlie"
    assert results[3][0] == "delta"

def test_memtable_overwrite_existing_keys_should_be_ok(sstable_path):
    memtable = MemTable(max_size=100)
    
    # Add initial value
    memtable.add("key1", "value1")
    assert memtable.get("key1") == "value1"
    
    # Overwrite with new value
    memtable.add("key1", "value2")
    assert memtable.get("key1") == "value2"
    
    # Verify only one entry exists
    assert len(memtable.entries) == 1
    
    # Write to SSTable and verify
    sstable = SSTable(sstable_path)
    sstable.write_memtable(memtable)
    
    # Verify only latest value is in SSTable
    assert sstable.get("key1") == "value2"
    assert len(sstable.index) == 1