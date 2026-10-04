import random
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from nimbus_kv.client import NimbusClient
from nimbus_kv.config import ClusterConfig

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ClusterIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        base_port = random.randint(22000, 45000)
        cls.ports = [base_port, base_port + 1, base_port + 2]
        cls.nodes = ",".join(
            f"n{i}=127.0.0.1:{port}" for i, port in enumerate(cls.ports, start=1)
        )
        cls.cluster = ClusterConfig.parse_nodes(cls.nodes)
        cls.processes = []
        for node_id in cls.cluster.node_ids:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "nimbus_kv.cli",
                    "server",
                    "--id",
                    node_id,
                    "--nodes",
                    cls.nodes,
                    "--data-dir",
                    cls.tmp.name,
                    "--election-min",
                    "0.4",
                    "--election-max",
                    "0.7",
                    "--heartbeat",
                    "0.15",
                    "--rpc-timeout",
                    "0.5",
                ],
                cwd=PROJECT_ROOT,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )
            cls.processes.append((node_id, process))

        cls.client = NimbusClient(cls.cluster, timeout=4)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            statuses = cls.client.status()
            if any(item.get("state") == "leader" for item in statuses):
                break
            time.sleep(0.2)
        else:
            cls.tearDownClass()
            raise RuntimeError("cluster did not elect a leader in time")

    @classmethod
    def tearDownClass(cls):
        for _, process in cls.processes:
            if process.poll() is None:
                process.terminate()
        for _, process in cls.processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        cls.tmp.cleanup()

    def _current_leader(self) -> str:
        statuses = self.client.status()
        leader = next(
            (item["id"] for item in statuses if item.get("state") == "leader"), None
        )
        self.assertIsNotNone(leader)
        return leader

    def test_write_read_and_failover(self):
        self.assertEqual(self.client.put("hero", "nimbus"), "nimbus")
        self.assertEqual(self.client.get("hero"), "nimbus")
        self.assertIn(["hero", "nimbus"], self.client.list())

        leader = self._current_leader()
        leader_process = next(process for node_id, process in self.processes if node_id == leader)
        leader_process.terminate()
        leader_process.wait(timeout=5)

        deadline = time.monotonic() + 8
        new_leader = None
        while time.monotonic() < deadline:
            statuses = self.client.status()
            new_leader = next(
                (
                    item["id"]
                    for item in statuses
                    if item.get("state") == "leader"
                    and item.get("id") != leader
                ),
                None,
            )
            if new_leader:
                break
            time.sleep(0.2)
        self.assertIsNotNone(new_leader)
        self.assertEqual(self.client.get("hero"), "nimbus")
        self.assertEqual(self.client.put("after-failover", "ok"), "ok")


if __name__ == "__main__":
    unittest.main()
