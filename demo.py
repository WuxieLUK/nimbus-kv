"""One-command interactive demo for NimbusKV.

The demo starts a real three-node cluster, writes and reads data, kills the
current leader, waits for failover, and reads the data again. It also prints
the exact client commands an interviewer can reuse in their own terminal.

Usage:
    python demo.py
    python demo.py --record-file demo_frames.json
"""
from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from nimbus_kv.client import ClientError, NimbusClient  # noqa: E402
from nimbus_kv.config import ClusterConfig  # noqa: E402

COLORS = {
    "green": "\033[92m",
    "cyan": "\033[96m",
    "yellow": "\033[93m",
    "red": "\033[91m",
    "gray": "\033[90m",
    "bold": "\033[1m",
    "reset": "\033[0m",
}


class DemoRecorder:
    """Keep the last few plain-text terminal frames for GIF generation."""

    def __init__(self, path: str | None) -> None:
        self.path = path
        self.frames: list[str] = []

    def add(self, text: str) -> None:
        if self.path:
            self.frames.append(text)

    def save(self) -> None:
        if self.path:
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            Path(self.path).write_text(json.dumps(self.frames, indent=2), encoding="utf-8")


class Demo:
    def __init__(self, record_file: str | None = None, port_base: int | None = None) -> None:
        self.record_file = record_file
        self.port_base = port_base or random.randint(28000, 45000)
        self.ports = [self.port_base, self.port_base + 1, self.port_base + 2]
        self.nodes = ",".join(
            f"n{i}=127.0.0.1:{port}" for i, port in enumerate(self.ports, 1)
        )
        self.cluster = ClusterConfig.parse_nodes(self.nodes)
        self.client = NimbusClient(self.cluster, timeout=5)
        self.processes: list[subprocess.Popen] = []
        self.tmp: tempfile.TemporaryDirectory | None = None
        self.screen: list[str] = []
        self.recorder = DemoRecorder(record_file)

    # ------------------------------------------------------------------
    def emit(self, line: str = "", color: str | None = None, delay: float = 0.7) -> None:
        self.screen.append(line)
        text = "\n".join(self.screen[-20:])
        self.recorder.add(text)
        if color:
            line = f"{COLORS[color]}{line}{COLORS['reset']}"
        print(line, flush=True)
        time.sleep(delay)

    def cmd(self, args: list[str], delay: float = 0.8) -> str:
        command = "$ " + " ".join(args)
        self.emit(command, color="green", delay=delay)
        result = subprocess.run(
            args,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        output = (result.stdout or "").strip()
        if result.returncode != 0 and result.stderr:
            output += ("\n" + result.stderr.strip()).strip()
        for line in output.splitlines() or ["(no output)"]:
            self.emit(line, color="gray", delay=0.15)
        return output

    def client_cmd(self, action: str, *extra: str) -> str:
        args = [
            sys.executable,
            "-m",
            "nimbus_kv.cli",
            "client",
            "--nodes",
            self.nodes,
            action,
            "--timeout",
            "6",
            *extra,
        ]
        return self.cmd(args, delay=0.6)

    # ------------------------------------------------------------------
    def start(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        for node_id in self.cluster.node_ids:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "nimbus_kv.cli",
                    "server",
                    "--id",
                    node_id,
                    "--nodes",
                    self.nodes,
                    "--data-dir",
                    self.tmp.name,
                    "--election-min",
                    "0.45",
                    "--election-max",
                    "0.75",
                    "--heartbeat",
                    "0.18",
                    "--rpc-timeout",
                    "0.6",
                ],
                cwd=PROJECT_ROOT,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )
            self.processes.append(process)

    def stop(self) -> None:
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
        for process in self.processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        if self.tmp:
            self.tmp.cleanup()

    def wait_for_leader(self, exclude: set[str] | None = None, timeout: float = 10) -> str:
        exclude = exclude or set()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for item in self.client.status():
                if item.get("state") == "leader" and item.get("id") not in exclude:
                    return item["id"]
            time.sleep(0.1)
        raise RuntimeError("no leader elected in time")

    def wait_until_serving(self, key: str, expected: str, timeout: float = 12) -> float:
        """Return seconds until the cluster can serve a linearizable read."""
        started = time.perf_counter()
        deadline = started + timeout
        while time.perf_counter() < deadline:
            try:
                if self.client.get(key) == expected:
                    return time.perf_counter() - started
            except ClientError:
                pass
            time.sleep(0.1)
        raise RuntimeError("cluster did not become readable in time")

    def status_lines(self) -> list[str]:
        rows = []
        for item in self.client.status():
            rows.append(
                f"{item.get('id', '?'):<6} {str(item.get('state', '?')):<10} "
                f"term={str(item.get('term', '?')):<3} leader={item.get('leader_id') or '-'}"
            )
        return rows

    # ------------------------------------------------------------------
    def run(self) -> int:
        self.emit("NimbusKV live demo", color="bold", delay=0.6)
        self.emit("starting a real 3-node Raft cluster...", color="cyan", delay=1.0)
        self.start()
        leader = self.wait_for_leader()
        self.emit(f"leader elected: {leader}", color="yellow", delay=0.8)
        self.emit("cluster status:", color="cyan", delay=0.3)
        for line in self.status_lines():
            self.emit(line, color="gray", delay=0.2)

        self.emit("", delay=0.2)
        self.client_cmd("put", "--key", "hero", "--value", "nimbus")
        self.client_cmd("get", "--key", "hero")
        self.client_cmd("list")

        self.emit("", delay=0.4)
        self.emit(f"killing leader {leader}...", color="red", delay=1.2)
        leader_process = next(
            process
            for node_id, process in zip(self.cluster.node_ids, self.processes)
            if node_id == leader
        )
        leader_process.terminate()
        leader_process.wait(timeout=5)

        self.emit("waiting for a new leader...", color="cyan", delay=1.0)
        elapsed = self.wait_until_serving("hero", "nimbus")
        new_leader = self.wait_for_leader(exclude={leader})
        self.emit(f"new leader: {new_leader}", color="yellow", delay=0.7)
        self.emit(f"failover time: {elapsed * 1000:.0f} ms", color="green", delay=0.7)

        self.emit("", delay=0.3)
        self.client_cmd("get", "--key", "hero")
        self.client_cmd("put", "--key", "after-failover", "--value", "ok")
        self.emit("data survived the leader crash", color="green", delay=0.7)

        self.stop()
        self.emit("", delay=0.2)
        self.emit("demo complete", color="bold", delay=0.4)
        self.recorder.save()
        return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the NimbusKV interview demo")
    parser.add_argument("--record-file", default=None, help="write terminal frames as JSON")
    parser.add_argument("--port-base", type=int, default=None)
    args = parser.parse_args(argv)
    demo = Demo(record_file=args.record_file, port_base=args.port_base)
    try:
        return demo.run()
    except KeyboardInterrupt:
        demo.stop()
        return 130
    finally:
        demo.stop()


if __name__ == "__main__":
    raise SystemExit(main())
