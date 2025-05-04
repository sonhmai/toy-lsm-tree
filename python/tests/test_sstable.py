import unittest
import os
import tempfile
import shutil

from lsm.sstable import SSTable
from lsm.memtable import MemTable

class TestSSTable(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for test files
        self.test_dir = tempfile.mkdtemp()
        self.sstable_path = os.path.join(self.test_dir, "test.sstable")
        
    def tearDown(self):
        # Clean up test directory
        shutil.rmtree(self.test_dir)

    def test_create_empty_sstable(self):
        """Test creating an empty SSTable without existing file"""
        sstable = SSTable(self.sstable_path)
        self.assertEqual(sstable.filename, self.sstable_path)
        self.assertEqual(sstable.index, {})
        
    def test_write_memtable_basic(self):
        """Test writing basic memtable data to SSTable"""
        # Create memtable with test data
        memtable = MemTable(max_size=1000)
        memtable.add("key1", "value1")
        memtable.add("key2", "value2")
        memtable.add("key3", "value3")
        
        # Write to SSTable
        sstable = SSTable(self.sstable_path)
        sstable.write_memtable(memtable)
        
        # Verify index was built correctly
        self.assertEqual(len(sstable.index), 3)
        self.assertIn("key1", sstable.index)
        self.assertIn("key2", sstable.index)
        self.assertIn("key3", sstable.index)
        
    def test_get_from_sstable(self):
        """Test reading values from SSTable"""
        # Setup data
        memtable = MemTable(max_size=1000)
        memtable.add("apple", "red")
        memtable.add("banana", "yellow")
        
        # Write and create new instance to test loading
        sstable = SSTable(self.sstable_path)
        sstable.write_memtable(memtable)
        
        # Create new SSTable instance to test loading
        # Note: This test will fail due to the file handling bug
        loaded_sstable = SSTable(self.sstable_path)
        
        # Test getting values
        self.assertEqual(loaded_sstable.get("apple"), "red")
        self.assertEqual(loaded_sstable.get("banana"), "yellow")
        self.assertIsNone(loaded_sstable.get("grape"))
        
    def test_range_scan(self):
        """Test range scanning functionality"""
        # Create test data
        memtable = MemTable(max_size=1000)
        memtable.add("apple", 1)
        memtable.add("banana", 2)
        memtable.add("cherry", 3)
        memtable.add("date", 4)
        memtable.add("elderberry", 5)
        
        # Write to SSTable
        sstable = SSTable(self.sstable_path)
        sstable.write_memtable(memtable)
        
        # Test range scan
        results = list(sstable.range_scan("banana", "date"))
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0], ("banana", 2))
        self.assertEqual(results[1], ("cherry", 3))
        self.assertEqual(results[2], ("date", 4))
        
        # Test edge cases
        results = list(sstable.range_scan("a", "z"))
        self.assertEqual(len(results), 5)
        
        results = list(sstable.range_scan("z", "zz"))
        self.assertEqual(len(results), 0)
        
    def test_empty_range_scan(self):
        """Test range scan on empty SSTable"""
        sstable = SSTable(self.sstable_path)
        results = list(sstable.range_scan("a", "z"))
        self.assertEqual(len(results), 0)
        
    def test_file_format_compatibility(self):
        """Test file format by manually checking written file"""
        memtable = MemTable(max_size=1000)
        memtable.add("key", "value")
        
        sstable = SSTable(self.sstable_path)
        sstable.write_memtable(memtable)
        
        # Check if temp file exists (bug test)
        temp_file = f"{self.sstable_path}.sstable"
        self.assertTrue(os.path.exists(temp_file))
        self.assertFalse(os.path.exists(self.sstable_path))
        
    def test_error_handling_invalid_file(self):
        """Test error handling for corrupted files"""
        # Create corrupt file
        with open(self.sstable_path, "wb") as f:
            f.write(b"INVALID_DATA")
            
        # Test that loading fails gracefully
        with self.assertRaises(ValueError):
            SSTable(self.sstable_path)
            
    def test_complex_data_types(self):
        """Test with complex data types"""
        memtable = MemTable(max_size=1000)
        memtable.add("list_key", [1, 2, 3, 4])
        memtable.add("dict_key", {"nested": {"value": 42}})
        memtable.add("tuple_key", (1, 2, 3))
        
        sstable = SSTable(self.sstable_path)
        sstable.write_memtable(memtable)
        
        # Verify complex data can be stored and retrieved
        self.assertEqual(sstable.get("list_key"), [1, 2, 3, 4])
        self.assertEqual(sstable.get("dict_key"), {"nested": {"value": 42}})
        self.assertEqual(sstable.get("tuple_key"), (1, 2, 3))
        
    def test_large_dataset(self):
        """Test with larger dataset to check scaling"""
        memtable = MemTable(max_size=10000)
        
        # Add 100 entries
        for i in range(100):
            key = f"key_{i:03d}"
            value = f"value_{i}"
            memtable.add(key, value)
        
        sstable = SSTable(self.sstable_path)
        sstable.write_memtable(memtable)
        
        # Verify index size
        self.assertEqual(len(sstable.index), 100)
        
        # Verify random access
        self.assertEqual(sstable.get("key_050"), "value_50")
        
        # Verify range scan
        results = list(sstable.range_scan("key_010", "key_020"))
        self.assertEqual(len(results), 11)  # includes both boundaries
        
    def test_memtable_ordering(self):
        """Test that MemTable maintains key ordering"""
        memtable = MemTable(max_size=100)
        
        # Add keys in random order
        keys = ["charlie", "alpha", "beta", "delta"]
        for i, key in enumerate(keys):
            memtable.add(key, i)
        
        # Verify keys are sorted in memtable
        entries = list(memtable.entries)
        self.assertEqual(entries[0][0], "alpha")
        self.assertEqual(entries[1][0], "beta")
        self.assertEqual(entries[2][0], "charlie")
        self.assertEqual(entries[3][0], "delta")
        
        # Write to SSTable and verify ordering
        sstable = SSTable(self.sstable_path)
        sstable.write_memtable(memtable)
        
        # Verify keys are still sorted in SSTable
        sorted_keys = sorted(sstable.index.keys())
        self.assertEqual(sorted_keys, ["alpha", "beta", "charlie", "delta"])
        
    def test_memtable_overwrite(self):
        """Test overwriting existing keys in MemTable"""
        memtable = MemTable(max_size=100)
        
        # Add initial value
        memtable.add("key1", "value1")
        self.assertEqual(memtable.get("key1"), "value1")
        
        # Overwrite with new value
        memtable.add("key1", "value2")
        self.assertEqual(memtable.get("key1"), "value2")
        
        # Verify only one entry exists
        self.assertEqual(len(memtable.entries), 1)
        
        # Write to SSTable and verify
        sstable = SSTable(self.sstable_path)
        sstable.write_memtable(memtable)
        
        # Verify only latest value is in SSTable
        self.assertEqual(sstable.get("key1"), "value2")
        self.assertEqual(len(sstable.index), 1)

if __name__ == "__main__":
    unittest.main()