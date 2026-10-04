import unittest

from nimbus_kv.state_machine import CommandError, KVStateMachine


class KVStateMachineTest(unittest.TestCase):
    def test_put_get_delete(self):
        machine = KVStateMachine()
        machine.apply({"op": "put", "key": "a", "value": "1"})
        self.assertEqual(machine.get("a"), "1")
        machine.apply({"op": "delete", "key": "a"})
        self.assertIsNone(machine.get("a"))

    def test_snapshot_roundtrip(self):
        machine = KVStateMachine()
        machine.apply({"op": "put", "key": "a", "value": "1"})
        restored = KVStateMachine.from_snapshot(machine.snapshot())
        self.assertEqual(restored.get("a"), "1")

    def test_invalid_command(self):
        machine = KVStateMachine()
        with self.assertRaises(CommandError):
            machine.apply({"op": "nope"})


if __name__ == "__main__":
    unittest.main()
