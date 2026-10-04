"""TCP RPC server and optional HTTP status dashboard."""
from __future__ import annotations

import html
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from socketserver import BaseRequestHandler, ThreadingTCPServer
from typing import Any

from .config import ClusterConfig
from .persistent import RaftStorage
from .raft import NotLeaderError, ProposalTimeout, RaftNode
from .rpc import RPCError, receive_message, send_message


class NimbusServer:
    """Wraps a Raft node with a network endpoint and lifecycle management."""

    def __init__(
        self,
        node_id: str,
        cluster: ClusterConfig,
        data_dir: str = "data",
        http_port: int | None = None,
    ) -> None:
        self.node_id = node_id
        self.cluster = cluster
        config = cluster.nodes[node_id]
        storage = RaftStorage(
            cluster.data_dir(node_id, data_dir), config.snapshot_threshold
        )
        self.node = RaftNode(node_id, cluster, storage)

        class RPCRequestHandler(BaseRequestHandler):
            def handle(self) -> None:
                dispatcher = self.server.dispatcher  # type: ignore[attr-defined]
                dispatcher._handle_connection(self.request)

        self.rpc_server = ThreadingTCPServer((config.host, config.port), RPCRequestHandler)
        self.rpc_server.daemon_threads = True
        self.rpc_server.dispatcher = self  # type: ignore[attr-defined]

        self.http_port = http_port
        self.dashboard: ThreadingHTTPServer | None = None
        if http_port is not None:
            self.dashboard = self._build_dashboard(config.host, http_port)

    @staticmethod
    def _build_dashboard(host: str, port: int) -> ThreadingHTTPServer:
        class DashboardHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                owner = self.server.owner  # type: ignore[attr-defined]
                if self.path == "/status":
                    body = json.dumps(owner._cluster_status(), ensure_ascii=False).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if self.path == "/health":
                    body = b"ok\n"
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if self.path == "/":
                    body = owner._dashboard_html().encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                self.send_error(404)

            def log_message(self, format: str, *args: Any) -> None:
                return

        server = ThreadingHTTPServer((host, port), DashboardHandler)
        server.daemon_threads = True
        server.owner = None  # type: ignore[attr-defined]
        return server

    def start(self) -> None:
        self.node.start()
        threading.Thread(target=self.rpc_server.serve_forever, daemon=True).start()
        if self.dashboard is not None:
            self.dashboard.owner = self  # type: ignore[attr-defined]
            threading.Thread(target=self.dashboard.serve_forever, daemon=True).start()

    def stop(self) -> None:
        self.rpc_server.shutdown()
        if self.dashboard is not None:
            self.dashboard.shutdown()
        self.node.stop()
        self.node.join(timeout=2.0)

    def _handle_connection(self, sock: Any) -> None:
        try:
            message = receive_message(sock)
        except RPCError:
            return
        response = self._dispatch(message)
        try:
            send_message(sock, response)
        except RPCError:
            pass
        finally:
            sock.close()

    def _dispatch(self, message: dict[str, Any]) -> dict[str, Any]:
        message_type = message.get("type")
        payload = message.get("payload", {})
        if message_type == "request_vote":
            return {
                "type": "request_vote_response",
                "payload": self.node.handle_request_vote(payload),
            }
        if message_type == "append_entries":
            return {
                "type": "append_entries_response",
                "payload": self.node.handle_append_entries(payload),
            }
        if message_type == "install_snapshot":
            return {
                "type": "install_snapshot_response",
                "payload": self.node.handle_install_snapshot(payload),
            }
        if message_type == "status_request":
            return {"type": "status_response", "payload": self._status_payload()}
        if message_type == "client_request":
            return self._dispatch_client_request(payload)
        return {
            "type": "client_response",
            "payload": {
                "request_id": payload.get("request_id"),
                "ok": False,
                "error": f"unknown message type {message_type!r}",
            },
        }

    def _dispatch_client_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        request_id = payload.get("request_id")
        op = payload.get("op")
        key = payload.get("key")
        value = payload.get("value")

        def success(value: Any = None) -> dict[str, Any]:
            return {
                "type": "client_response",
                "payload": {"request_id": request_id, "ok": True, "value": value},
            }

        def failure(message: str) -> dict[str, Any]:
            return {
                "type": "client_response",
                "payload": {
                    "request_id": request_id,
                    "ok": False,
                    "error": message,
                },
            }

        try:
            if op == "put":
                return success(self.node.propose({"op": "put", "key": key, "value": value}))
            if op == "delete":
                return success(self.node.propose({"op": "delete", "key": key}))
            if op == "get":
                return success(self.node.linearizable_read(str(key)))
            if op == "list":
                with self.node.condition:
                    if self.node.state != RaftNode.LEADER:
                        raise NotLeaderError(
                            self.node.leader_id, self.node._leader_address(self.node.leader_id)
                        )
                    return success(self.node.state_machine.items())
            return failure(f"unsupported client operation: {op!r}")
        except NotLeaderError as exc:
            leader_address = exc.leader_address or (None, None)
            return {
                "type": "client_response",
                "payload": {
                    "request_id": request_id,
                    "ok": False,
                    "error": "not_leader",
                    "leader_id": exc.leader_id,
                    "leader_host": leader_address[0],
                    "leader_port": leader_address[1],
                },
            }
        except ProposalTimeout as exc:
            return failure(str(exc))
        except Exception as exc:  # pragma: no cover - defensive boundary
            return failure(f"{type(exc).__name__}: {exc}")

    def _status_payload(self) -> dict[str, Any]:
        status = self.node.status()
        status["address"] = self.cluster.as_mapping().get(self.node_id)
        return status

    def _cluster_status(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for node_id in self.cluster.node_ids:
            host, port = self.cluster.address(node_id)
            if node_id == self.node_id:
                result.append(self._status_payload())
                continue
            try:
                from .rpc import Address, call

                response = call(
                    Address(host, port),
                    {"type": "status_request", "payload": {}},
                    timeout=self.cluster.nodes[node_id].rpc_timeout,
                )["payload"]
                result.append(response)
            except RPCError:
                result.append({"id": node_id, "state": "unreachable", "address": {"host": host, "port": port}})
        return result

    def _dashboard_html(self) -> str:
        rows = []
        for item in self._cluster_status():
            state = html.escape(str(item.get("state", "unknown")))
            node_id = html.escape(str(item.get("id", "?")))
            leader = html.escape(str(item.get("leader_id", "")))
            term = html.escape(str(item.get("term", "")))
            commit = html.escape(str(item.get("commit_index", "")))
            rows.append(
                f"<tr><td>{node_id}</td><td>{state}</td><td>{term}</td>"
                f"<td>{leader}</td><td>{commit}</td></tr>"
            )
        return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>NimbusKV</title>
<style>
body{{font-family:system-ui,Segoe UI,sans-serif;margin:40px;background:#0f172a;color:#e2e8f0}}
h1{{font-weight:600}} table{{border-collapse:collapse;min-width:640px;background:#1e293b}}
th,td{{border:1px solid #334155;padding:10px 14px;text-align:left}}
th{{background:#111827}} .leader{{color:#4ade80;font-weight:600}}
</style></head><body><h1>NimbusKV cluster</h1>
<table><thead><tr><th>Node</th><th>State</th><th>Term</th><th>Leader</th><th>Commit</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table></body></html>"""
