import unittest

from nimbus_kv.log import LogEntry, RaftLog


class RaftLogTest(unittest.TestCase):
    def test_append_is_one_based(self):
        log = RaftLog()
        first = log.append_command(1, {"op": "put", "key": "a", "value": "1"})
        second = log.append_command(2, {"op": "put", "key": "b", "value": "2"})
        self.assertEqual(first.index, 1)
        self.assertEqual(second.index, 2)
        self.assertEqual(log.last_index, 2)
        self.assertEqual(log.last_term, 2)

    def test_term_at(self):
        log = RaftLog([LogEntry(1, 1, {}), LogEntry(1, 2, {}), LogEntry(2, 3, {})])
        self.assertEqual(log.term_at(1), 1)
        self.assertEqual(log.term_at(2), 1)
        self.assertEqual(log.term_at(3), 2)
        self.assertIsNone(log.term_at(4))

    def test_truncate_conflict(self):
        log = RaftLog([LogEntry(1, 1, {}), LogEntry(1, 2, {}), LogEntry(1, 3, {})])
        removed = log.truncate_from(2)
        self.assertEqual([entry.index for entry in removed], [2, 3])
        self.assertEqual(log.last_index, 1)

    def test_base_index_after_snapshot(self):
        log = RaftLog(base_index=10)
        self.assertEqual(log.last_index, 10)
        entry = log.append_command(3, {"op": "put", "key": "x", "value": "y"})
        self.assertEqual(entry.index, 11)
        self.assertEqual(log.slice_from(11), [entry])


if __name__ == "__main__":
    unittest.main()
