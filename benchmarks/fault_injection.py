"""Fault-injection benchmark for NimbusKV.

Spawns a three-node cluster, kills the current leader on every round, and
measures how long a linearizable write takes to succeed after failover.
"""
from __future__ import annotations

import argparse
import random
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from nimbus_kv.client import NimbusClient  # noqa: E402
from nimbus_kv.config import ClusterConfig  # noqa: E402


def wait_for_leader(client: NimbusClient, exclude: set[str] | None = None, timeout: float = 10) -> str:
    exclude = exclude or set()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        statuses = client.status()
        for item in statuses:
            if item.get("state") == "leader" and item.get("id") not in exclude:
                return item["id"]
        time.sleep(0.1)
    raise RuntimeError("no leader elected in time")


def wait_for_reachable(client: NimbusClient, node_id: str, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for item in client.status():
            if item.get("id") == node_id and item.get("state") != "unreachable":
                return
        time.sleep(0.1)
    raise RuntimeError(f"node {node_id} did not become reachable in time")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--port-base", type=int, default=0)
    parser.add_argument("--election-min", type=float, default=0.4)
    parser.add_argument("--election-max", type=float, default=0.7)
    parser.add_argument("--heartbeat", type=float, default=0.15)
    args = parser.parse_args(argv)

    base_port = args.port_base or random.randint(24000, 45000)
    ports = [base_port, base_port + 1, base_port + 2]
    nodes = ",".join(f"n{i}=127.0.0.1:{port}" for i, port in enumerate(ports, 1))
    cluster = ClusterConfig.parse_nodes(nodes)
    client = NimbusClient(cluster, timeout=5)

    with tempfile.TemporaryDirectory() as data_dir:
        def start_node(node_id: str) -> subprocess.Popen:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "nimbus_kv.cli",
                    "server",
                    "--id",
                    node_id,
                    "--nodes",
                    nodes,
                    "--data-dir",
                    data_dir,
                    "--election-min",
                    str(args.election_min),
                    "--election-max",
                    str(args.election_max),
                    "--heartbeat",
                    str(args.heartbeat),
                    "--rpc-timeout",
                    "0.5",
                ],
                cwd=PROJECT_ROOT,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )
            return process

        processes = {node_id: start_node(node_id) for node_id in cluster.node_ids}

        try:
            wait_for_leader(client)
            print(f"cluster ready on ports {ports[0]}-{ports[-1]}")
            outages = []
            for round_no in range(1, args.rounds + 1):
                leader = wait_for_leader(client)
                process = processes[leader]
                process.terminate()
                process.wait(timeout=5)
                started = time.perf_counter()
                new_leader = wait_for_leader(client, exclude={leader})
                client.put(f"probe:{round_no}", f"value:{round_no}")
                outage = time.perf_counter() - started
                outages.append(outage)
                print(
                    f"round {round_no}: killed {leader}, new leader {new_leader}, "
                    f"recovery {outage * 1000:.0f} ms"
                )
                processes[leader] = start_node(leader)
                wait_for_reachable(client, leader)
            if outages:
                print(
                    f"summary: {args.rounds} failovers, "
                    f"avg recovery {sum(outages) / len(outages) * 1000:.0f} ms, "
                    f"max recovery {max(outages) * 1000:.0f} ms"
                )
            return 0
        finally:
            for process in processes.values():
                if process.poll() is None:
                    process.terminate()
            for process in processes.values():
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()


if __name__ == "__main__":
    raise SystemExit(main())
