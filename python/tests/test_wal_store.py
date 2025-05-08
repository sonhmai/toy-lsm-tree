# test_wal_store.py
import json
import os
import pickle
from pathlib import Path

import pytest

from lsm.wal_store import WalStore, WalEntry


def new_store(tmp_path: Path) -> WalStore:
    """Create a WalStore pointing at fresh files inside pytest's tmp dir."""
    data_file = tmp_path / "checkpoint.db"
    wal_file = tmp_path / "wal.log"
    return WalStore(str(data_file), str(wal_file))


def read_wal_lines(path: Path):
    if not path.exists():
        return []
    with open(path, "r") as f:
        return [json.loads(l) for l in f if l.strip()]

def test_set_operation_is_persisted_and_recovers(tmp_path: Path):
    store = new_store(tmp_path)
    store.set("user:1", {"name": "Alice"})
    store.set("user:2", {"name": "Bob"})

    # WAL should have two "set" entries
    wal_entries = read_wal_lines(tmp_path / "wal.log")
    assert [e["operation"] for e in wal_entries] == ["set", "set"]

    # Data is present in-memory
    assert store.data == {
        "user:1": {"name": "Alice"},
        "user:2": {"name": "Bob"},
    }

    # Restart → recovery should replay WAL
    recovered = new_store(tmp_path)
    assert recovered.data == store.data


def test_delete_operation_is_replayed(tmp_path: Path):
    store = new_store(tmp_path)
    store.set("x", 1)
    store.delete("x")

    # Latest state in memory
    assert "x" not in store.data

    # WAL should have set + delete
    wal_ops = [e["operation"] for e in read_wal_lines(tmp_path / "wal.log")]
    assert wal_ops == ["set", "delete"]

    # Recovery should reflect the delete
    recovered = new_store(tmp_path)
    assert "x" not in recovered.data


def test_checkpoint_persists_state_and_truncates_wal(tmp_path: Path):
    store = new_store(tmp_path)
    store.set("a", 100)
    store.set("b", 200)

    store.checkpoint()

    # WAL must be empty after checkpoint
    wal_path = tmp_path / "wal.log"
    assert wal_path.stat().st_size == 0

    # Checkpoint file should contain pickled dict with both keys
    data_path = tmp_path / "checkpoint.db"
    with open(data_path, "rb") as f:
        on_disk = pickle.load(f)
    assert on_disk == {"a": 100, "b": 200}

    # Recovery should come only from checkpoint (no WAL replay)
    recovered = new_store(tmp_path)
    assert recovered.data == on_disk


def test_corrupt_wal_line_raises_runtime_error(tmp_path: Path):
    # Prepare valid store + WAL
    store = new_store(tmp_path)
    store.set("valid", 1)

    # Append garbage
    with open(tmp_path / "wal.log", "a") as f:
        f.write("{{ this is not json }}\n")

    with pytest.raises(RuntimeError, match="Recovery failed"):
        _ = new_store(tmp_path)
