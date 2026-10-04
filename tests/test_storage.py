import tempfile
import unittest
from pathlib import Path

from nimbus_kv.log import LogEntry
from nimbus_kv.persistent import RaftStorage


class RaftStorageTest(unittest.TestCase):
    def test_roundtrip_metadata_and_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = RaftStorage(tmp)
            storage.save_meta(3, "node2")
            storage.append_entries(
                [LogEntry(3, 1, {"op": "put", "key": "a", "value": "b"})]
            )
            loaded = storage.load()
            self.assertEqual(loaded.current_term, 3)
            self.assertEqual(loaded.voted_for, "node2")
            self.assertEqual(loaded.entries[0].index, 1)

    def test_snapshot_replaces_wal(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = RaftStorage(tmp)
            storage.save_snapshot(4, 2, {"a": "b"})
            loaded = storage.load()
            self.assertEqual(loaded.snapshot_index, 4)
            self.assertEqual(loaded.state, {"a": "b"})
            self.assertEqual(loaded.entries, [])

    def test_truncate_wal(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = RaftStorage(tmp)
            storage.append_entries(
                [LogEntry(1, 1, {}), LogEntry(1, 2, {}), LogEntry(1, 3, {})]
            )
            storage.truncate_wal_from(2)
            loaded = storage.load()
            self.assertEqual([entry.index for entry in loaded.entries], [1])


if __name__ == "__main__":
    unittest.main()
