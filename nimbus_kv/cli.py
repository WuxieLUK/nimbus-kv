"""Command-line interface for NimbusKV."""
from __future__ import annotations

import argparse
import random
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from . import __version__
from .client import ClientError, NimbusClient
from .config import ClusterConfig
from .server import NimbusServer


def _cluster_from_args(args: argparse.Namespace) -> ClusterConfig:
    defaults: dict[str, Any] = {}
    if hasattr(args, "election_min"):
        defaults.update(
            {
                "election_timeout_min": args.election_min,
                "election_timeout_max": args.election_max,
                "heartbeat_interval": args.heartbeat,
                "snapshot_threshold": args.snapshot_threshold,
                "rpc_timeout": args.rpc_timeout,
            }
        )
    return ClusterConfig.parse_nodes(args.nodes, defaults)


def _run_server(args: argparse.Namespace) -> int:
    if args.id not in {item.split("=")[0] for item in args.nodes.split(",")}:
        print(f"error: --id {args.id!r} is not present in --nodes", flush=True)
        return 2
    cluster = _cluster_from_args(args)
    server = NimbusServer(
        args.id, cluster, data_dir=args.data_dir, http_port=args.http_port
    )
    server.start()
    host, port = cluster.address(args.id)
    print(
        f"node {args.id} listening on {host}:{port} "
        f"(http dashboard on port {args.http_port})",
        flush=True,
    )
    stop = threading.Event()

    def handle_signal(signum: int, frame: Any) -> None:
        print(f"\nstopping node {args.id}...", flush=True)
        stop.set()

    signal.signal(signal.SIGINT, handle_signal)
    try:
        signal.signal(signal.SIGTERM, handle_signal)
    except (AttributeError, ValueError):
        pass

    while not stop.is_set():
        stop.wait(1.0)
    server.stop()
    return 0


def _run_client(args: argparse.Namespace) -> int:
    cluster = _cluster_from_args(args)
    client = NimbusClient(cluster, timeout=args.timeout)
    try:
        if args.action == "put":
            value = client.put(args.key, args.value or "")
            print(value)
        elif args.action == "get":
            value = client.get(args.key)
            if value is None:
                print("(nil)")
            else:
                print(value)
        elif args.action == "delete":
            client.delete(args.key)
            print("deleted")
        elif args.action == "list":
            for key, value in client.list():
                print(f"{key}\t{value}")
        elif args.action == "status":
            _print_status(client.status())
    except ClientError as exc:
        print(f"error: {exc}", flush=True)
        return 1
    return 0


def _print_status(rows: list[dict[str, Any]]) -> None:
    print(f"{'ID':<12} {'STATE':<12} {'TERM':<6} {'LEADER':<12} {'COMMIT':<8}")
    for row in rows:
        print(
            f"{str(row.get('id','?')):<12} {str(row.get('state','?')):<12} "
            f"{str(row.get('term','?')):<6} {str(row.get('leader_id','') or ''):<12} "
            f"{str(row.get('commit_index','?')):<8}"
        )


def _run_benchmark(args: argparse.Namespace) -> int:
    cluster = _cluster_from_args(args)
    print(
        f"benchmark: {args.ops} operations, {args.clients} clients, "
        f"write_ratio={args.write_ratio}"
    )
    per_client = max(1, args.ops // args.clients)
    started = time.perf_counter()
    failures: list[str] = []

    def worker(worker_id: int) -> tuple[int, float]:
        client = NimbusClient(cluster, timeout=max(5.0, args.timeout))
        local_start = time.perf_counter()
        ops = 0
        for i in range(per_client):
            key = f"bench:{worker_id}:{i % args.keys}"
            if random.random() < args.write_ratio:
                client.put(key, f"value-{i}")
            else:
                client.get(key)
            ops += 1
        return ops, time.perf_counter() - local_start

    try:
        with ThreadPoolExecutor(max_workers=args.clients) as executor:
            futures = [executor.submit(worker, i) for i in range(args.clients)]
            total_ops = 0
            max_latency = 0.0
            for future in futures:
                ops, duration = future.result()
                total_ops += ops
                max_latency = max(max_latency, duration)
    except Exception as exc:  # noqa: BLE001 - benchmark reports and exits
        print(f"benchmark failed: {exc}")
        return 1

    elapsed = time.perf_counter() - started
    print(f"completed: {total_ops} ops in {elapsed:.2f}s")
    print(f"throughput: {total_ops / elapsed:.1f} ops/s")
    print(f"average latency: {elapsed / max(total_ops, 1) * 1000:.2f} ms")
    if failures:
        print(f"failures: {len(failures)}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nimbus-kv", description="NimbusKV: a Raft-based distributed key-value store"
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    server = subparsers.add_parser("server", help="run a cluster node")
    server.add_argument("--id", required=True)
    server.add_argument("--nodes", required=True, help="node1=host:port,node2=host:port")
    server.add_argument("--data-dir", default="data")
    server.add_argument("--http-port", type=int, default=None)
    server.add_argument("--election-min", type=float, default=0.15)
    server.add_argument("--election-max", type=float, default=0.30)
    server.add_argument("--heartbeat", type=float, default=0.05)
    server.add_argument("--snapshot-threshold", type=int, default=500)
    server.add_argument("--rpc-timeout", type=float, default=1.0)
    server.set_defaults(func=_run_server)

    client = subparsers.add_parser("client", help="interact with a cluster")
    client.add_argument("action", choices=["put", "get", "delete", "list", "status"])
    client.add_argument("--nodes", required=True, help="node1=host:port,node2=host:port")
    client.add_argument("--key")
    client.add_argument("--value")
    client.add_argument("--timeout", type=float, default=2.0)
    client.set_defaults(func=_run_client)

    benchmark = subparsers.add_parser("benchmark", help="run a synthetic load")
    benchmark.add_argument("--nodes", required=True, help="node1=host:port,node2=host:port")
    benchmark.add_argument("--ops", type=int, default=1000)
    benchmark.add_argument("--clients", type=int, default=8)
    benchmark.add_argument("--keys", type=int, default=1000)
    benchmark.add_argument("--write-ratio", type=float, default=0.2)
    benchmark.add_argument("--timeout", type=float, default=2.0)
    benchmark.set_defaults(func=_run_benchmark)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
