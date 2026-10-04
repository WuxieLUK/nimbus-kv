"""Thin client with automatic leader discovery and retries."""
from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass
from typing import Any

from .config import ClusterConfig
from .rpc import Address, RPCError, call


class ClientError(RuntimeError):
    """The cluster could not serve the requested operation."""


@dataclass
class ClusterSnapshot:
    nodes: list[dict[str, Any]]


class NimbusClient:
    """Issue KV operations against a NimbusKV cluster.

    The client does not need to know the leader. If a node returns a redirect,
    the client follows the leader hint and retries until the deadline expires.
    """

    def __init__(self, cluster: ClusterConfig, timeout: float = 2.0) -> None:
        self.cluster = cluster
        self.timeout = timeout

    def _request(self, op: str, key: str | None = None, value: str | None = None) -> Any:
        request_id = uuid.uuid4().hex
        payload: dict[str, Any] = {"op": op, "request_id": request_id}
        if key is not None:
            payload["key"] = key
        if value is not None:
            payload["value"] = value

        node_ids = list(self.cluster.node_ids)
        random.shuffle(node_ids)
        deadline = time.monotonic() + self.timeout

        while time.monotonic() < deadline:
            for node_id in list(node_ids):
                try:
                    response = call(
                        Address(*self.cluster.address(node_id)),
                        {"type": "client_request", "payload": payload},
                        timeout=min(self.timeout, max(0.2, deadline - time.monotonic())),
                    )["payload"]
                except RPCError:
                    continue

                if response.get("ok"):
                    return response.get("value")

                if response.get("error") == "not_leader":
                    leader_id = response.get("leader_id")
                    if leader_id and leader_id in self.cluster.nodes:
                        if leader_id in node_ids:
                            node_ids.remove(leader_id)
                        node_ids.insert(0, leader_id)
                        break
                    continue

                # A deterministic error from the leader should not be retried.
                raise ClientError(str(response.get("error")))
            time.sleep(0.02)

        raise ClientError(
            f"cluster did not respond within {self.timeout:.1f}s "
            f"while executing {op}"
        )

    def put(self, key: str, value: str) -> str:
        return self._request("put", key=key, value=value)

    def get(self, key: str) -> str | None:
        return self._request("get", key=key)

    def delete(self, key: str) -> None:
        return self._request("delete", key=key)

    def list(self) -> list[list[str]]:
        return self._request("list")

    def status(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for node_id in self.cluster.node_ids:
            try:
                response = call(
                    Address(*self.cluster.address(node_id)),
                    {"type": "status_request", "payload": {}},
                    timeout=self.timeout,
                )["payload"]
                result.append(response)
            except RPCError:
                result.append(
                    {
                        "id": node_id,
                        "state": "unreachable",
                        "address": dict(zip(("host", "port"), self.cluster.address(node_id))),
                    }
                )
        return result
