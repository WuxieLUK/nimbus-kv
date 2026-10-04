"""Length-prefixed JSON RPC used for both peer and client communication.

The protocol is intentionally dependency-free: each message is four bytes of
big-endian length followed by a UTF-8 JSON object.
"""
from __future__ import annotations

import json
import socket
from dataclasses import dataclass
from typing import Any


MESSAGE_TYPES = {
    "request_vote",
    "request_vote_response",
    "append_entries",
    "append_entries_response",
    "install_snapshot",
    "install_snapshot_response",
    "client_request",
    "client_response",
    "status_request",
    "status_response",
}

MAX_MESSAGE_BYTES = 64 * 1024 * 1024


class RPCError(RuntimeError):
    """Transport-level RPC failure."""


class RPCTimeout(RPCError):
    """Timed out waiting for an RPC response."""


@dataclass
class Address:
    host: str
    port: int


def send_message(sock: socket.socket, message: dict[str, Any]) -> None:
    payload = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(payload) > MAX_MESSAGE_BYTES:
        raise RPCError(f"message too large: {len(payload)} bytes")
    sock.sendall(len(payload).to_bytes(4, "big"))
    sock.sendall(payload)


def receive_message(sock: socket.socket) -> dict[str, Any]:
    length_bytes = _recv_exact(sock, 4)
    if not length_bytes:
        raise RPCError("connection closed before message length")
    length = int.from_bytes(length_bytes, "big")
    if length <= 0 or length > MAX_MESSAGE_BYTES:
        raise RPCError(f"invalid message length: {length}")
    payload = _recv_exact(sock, length)
    if len(payload) != length:
        raise RPCError("connection closed in the middle of a message")
    return json.loads(payload.decode("utf-8"))


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def call(address: Address, message: dict[str, Any], timeout: float) -> dict[str, Any]:
    """Send one request and receive one response over a fresh TCP connection."""
    try:
        sock = socket.create_connection((address.host, address.port), timeout=timeout)
    except (socket.timeout, TimeoutError) as exc:
        raise RPCTimeout(f"RPC to {address.host}:{address.port} timed out") from exc
    except OSError as exc:
        raise RPCError(f"RPC to {address.host}:{address.port} failed: {exc}") from exc
    try:
        sock.settimeout(timeout)
        send_message(sock, message)
        response = receive_message(sock)
    except (socket.timeout, TimeoutError) as exc:
        raise RPCTimeout(f"RPC to {address.host}:{address.port} timed out") from exc
    except OSError as exc:
        raise RPCError(f"RPC to {address.host}:{address.port} failed: {exc}") from exc
    finally:
        sock.close()
    return response


def make_raft_message(kind: str, payload: dict[str, Any]) -> dict[str, Any]:
    if kind not in MESSAGE_TYPES:
        raise ValueError(f"unknown message type: {kind}")
    return {"type": kind, "payload": payload}
