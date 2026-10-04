"""Cluster and node configuration."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Tuple


@dataclass(frozen=True)
class NodeConfig:
    """Address and timing configuration for one cluster node."""

    node_id: str
    host: str = "127.0.0.1"
    port: int = 8000
    election_timeout_min: float = 0.15
    election_timeout_max: float = 0.30
    heartbeat_interval: float = 0.05
    snapshot_threshold: int = 500
    rpc_timeout: float = 1.0


@dataclass
class ClusterConfig:
    """All nodes in the cluster, indexed by node id."""

    nodes: Dict[str, NodeConfig] = field(default_factory=dict)

    @property
    def node_ids(self) -> list[str]:
        return list(self.nodes)

    @property
    def quorum_size(self) -> int:
        return len(self.nodes) // 2 + 1

    def address(self, node_id: str) -> Tuple[str, int]:
        node = self.nodes[node_id]
        return node.host, node.port

    def as_mapping(self) -> dict:
        return {
            node_id: {"host": node.host, "port": node.port}
            for node_id, node in self.nodes.items()
        }

    @classmethod
    def parse_nodes(cls, raw: str, defaults: dict | None = None) -> "ClusterConfig":
        """Parse ``node1=host:port,node2=host:port`` into a cluster config.

        Timing options can be supplied through ``defaults`` and are applied to
        every node, which keeps demo and test invocations short.
        """
        defaults = defaults or {}
        nodes: dict[str, NodeConfig] = {}
        for item in raw.split(","):
            item = item.strip()
            if not item:
                continue
            node_id, _, addr = item.partition("=")
            host, _, port_text = addr.rpartition(":")
            if not node_id or not host or not port_text:
                raise ValueError(f"invalid node spec: {item!r}")
            nodes[node_id] = NodeConfig(
                node_id=node_id.strip(),
                host=host.strip(),
                port=int(port_text),
                **defaults,
            )
        return cls(nodes=nodes)

    def data_dir(self, node_id: str, base_dir: str | Path = "data") -> Path:
        return Path(base_dir) / node_id
