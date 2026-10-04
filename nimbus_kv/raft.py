"""A compact Raft implementation with leader election, log replication,
snapshots, and linearizable reads.

The implementation follows the core safety rules from Ongaro and Ousterhout's
paper. It intentionally uses only the Python standard library and keeps the
state machine behind a small interface so it is easy to audit and extend.
"""
from __future__ import annotations

import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .config import ClusterConfig, NodeConfig
from .log import LogEntry, RaftLog
from .persistent import RaftStorage
from .rpc import Address, RPCTimeout, RPCError, call, make_raft_message
from .state_machine import KVStateMachine


class NotLeaderError(RuntimeError):
    """The contacted node is not the current leader."""

    def __init__(self, leader_id: str | None = None, leader_address: tuple[str, int] | None = None) -> None:
        self.leader_id = leader_id
        self.leader_address = leader_address
        message = "not the leader"
        if leader_id:
            message += f"; leader is {leader_id}"
        if leader_address:
            message += f" ({leader_address[0]}:{leader_address[1]})"
        super().__init__(message)


class ProposalTimeout(RuntimeError):
    """A write could not be committed within the client deadline."""


class RaftNode(threading.Thread):
    """Raft consensus node backed by a durable WAL and snapshot files."""

    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

    def __init__(
        self,
        node_id: str,
        cluster: ClusterConfig,
        storage: RaftStorage,
        state_machine: KVStateMachine | None = None,
    ) -> None:
        super().__init__(name=f"raft-{node_id}", daemon=True)
        self.node_id = node_id
        self.cluster = cluster
        self.storage = storage
        self.config: NodeConfig = cluster.nodes[node_id]

        loaded = storage.load()
        self.current_term = loaded.current_term
        self.voted_for = loaded.voted_for
        self.snapshot_index = loaded.snapshot_index
        self.snapshot_term = loaded.snapshot_term
        self.log = RaftLog(
            [entry for entry in loaded.entries if entry.index > loaded.snapshot_index],
            base_index=loaded.snapshot_index,
        )
        self.state_machine = state_machine or KVStateMachine.from_snapshot(loaded.state)

        self.state = self.FOLLOWER
        self.leader_id: str | None = None
        self.commit_index = self.snapshot_index
        self.last_applied = self.snapshot_index

        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self._votes_received: set[str] = set()
        self._replicating: set[str] = set()

        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        self.stop_event = threading.Event()
        self.election_deadline = time.monotonic() + self._election_timeout()
        self.last_heartbeat_sent = 0.0

    # ------------------------------------------------------------------
    # Persistence and log helpers
    # ------------------------------------------------------------------
    def _election_timeout(self) -> float:
        return random.uniform(
            self.config.election_timeout_min, self.config.election_timeout_max
        )

    def _reset_election_deadline_locked(self) -> None:
        self.election_deadline = time.monotonic() + self._election_timeout()

    def _persist_meta_locked(self) -> None:
        self.storage.save_meta(self.current_term, self.voted_for)

    def _last_log_index(self) -> int:
        return self.log.last_index

    def _last_log_term(self) -> int:
        index = self._last_log_index()
        if index == 0:
            return 0
        return self._term_at_global(index) or 0

    def _term_at_global(self, raft_index: int) -> int | None:
        if raft_index <= 0:
            return 0
        if raft_index == self.snapshot_index:
            return self.snapshot_term
        if raft_index < self.snapshot_index:
            return None
        return self.log.term_at(raft_index)

    def _entry_at_global(self, raft_index: int) -> LogEntry | None:
        if raft_index <= self.snapshot_index:
            return None
        return self.log.entry_at(raft_index)

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------
    def _become_follower_locked(self, term: int) -> None:
        self.state = self.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.leader_id = None
        self._persist_meta_locked()
        self._reset_election_deadline_locked()
        self._votes_received.clear()
        self._replicating.clear()
        self.condition.notify_all()

    def _start_election_locked(self) -> None:
        self.current_term += 1
        self.voted_for = self.node_id
        self.state = self.CANDIDATE
        self.leader_id = None
        self._persist_meta_locked()
        self._reset_election_deadline_locked()
        self._votes_received = {self.node_id}

        term = self.current_term
        last_index = self._last_log_index()
        last_term = self._last_log_term()
        for peer_id in self._peers():
            threading.Thread(
                target=self._request_vote_worker,
                args=(peer_id, term, last_index, last_term),
                daemon=True,
            ).start()

    def _request_vote_worker(
        self, peer_id: str, term: int, last_index: int, last_term: int
    ) -> None:
        request = make_raft_message(
            "request_vote",
            {
                "term": term,
                "candidate_id": self.node_id,
                "last_log_index": last_index,
                "last_log_term": last_term,
            },
        )
        try:
            response = call(
                Address(*self.cluster.address(peer_id)),
                request,
                self.config.rpc_timeout,
            )["payload"]
        except RPCError:
            return
        with self.condition:
            self._handle_request_vote_response_locked(term, response, peer_id)

    def _handle_request_vote_response_locked(
        self, term: int, response: dict[str, Any], from_node: str
    ) -> None:
        if self.state != self.CANDIDATE or term != self.current_term:
            return
        if response.get("term", 0) > self.current_term:
            self._become_follower_locked(response["term"])
            return
        if response.get("vote_granted"):
            self._votes_received.add(from_node)
            if len(self._votes_received) >= self.cluster.quorum_size:
                self._become_leader_locked()

    def _become_leader_locked(self) -> None:
        self.state = self.LEADER
        self.leader_id = self.node_id
        last_index = self._last_log_index()
        for peer_id in self._peers():
            self.next_index[peer_id] = last_index + 1
            self.match_index[peer_id] = 0
        # Commit a no-op entry from the new term. This advances the commit
        # index past entries committed by a previous leader and prevents the
        # new leader from serving reads before they have been applied.
        noop = self.log.append_command(self.current_term, {"op": "noop"})
        self.storage.append_entry(noop)
        self._ensure_replication_locked()
        self.last_heartbeat_sent = 0.0
        self._commit_locked()
        self._apply_committed_locked()
        self._send_heartbeats_locked()
        self.condition.notify_all()

    def _peers(self) -> list[str]:
        return [node_id for node_id in self.cluster.node_ids if node_id != self.node_id]

    # ------------------------------------------------------------------
    # Raft RPC handlers (called by the network server)
    # ------------------------------------------------------------------
    def handle_request_vote(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.condition:
            term = int(payload["term"])
            if term < self.current_term:
                return {"term": self.current_term, "vote_granted": False}
            if term > self.current_term:
                self._become_follower_locked(term)

            candidate_id = payload["candidate_id"]
            last_log_index = int(payload["last_log_index"])
            last_log_term = int(payload["last_log_term"])
            log_is_current = (
                last_log_term > self._last_log_term()
                or (
                    last_log_term == self._last_log_term()
                    and last_log_index >= self._last_log_index()
                )
            )
            if (
                self.voted_for in (None, candidate_id)
                and log_is_current
            ):
                self.voted_for = candidate_id
                self._persist_meta_locked()
                return {"term": self.current_term, "vote_granted": True}
            return {"term": self.current_term, "vote_granted": False}

    def handle_append_entries(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.condition:
            term = int(payload["term"])
            if term < self.current_term:
                return {"term": self.current_term, "success": False}
            if term > self.current_term:
                self._become_follower_locked(term)
            elif self.state == self.CANDIDATE:
                self.state = self.FOLLOWER
                self.leader_id = None

            self.leader_id = payload["leader_id"]
            self._reset_election_deadline_locked()

            prev_log_index = int(payload["prev_log_index"])
            prev_log_term = int(payload["prev_log_term"])
            if prev_log_index > self._last_log_index():
                return {"term": self.current_term, "success": False}
            if prev_log_index > 0 and self._term_at_global(prev_log_index) != prev_log_term:
                return {"term": self.current_term, "success": False}

            raw_entries = payload.get("entries", [])
            new_entries = [LogEntry.from_dict(raw) for raw in raw_entries]
            to_persist: list[LogEntry] = []
            for entry in new_entries:
                if entry.index <= self.log.last_index:
                    if self._term_at_global(entry.index) != entry.term:
                        self.log.truncate_from(entry.index)
                        self.storage.truncate_wal_from(entry.index)
                if entry.index > self.log.last_index:
                    if entry.index != self.log.last_index + 1:
                        self.log.truncate_from(entry.index)
                        self.storage.truncate_wal_from(entry.index)
                    self.log.append(entry)
                    to_persist.append(entry)
            if to_persist:
                self.storage.append_entries(to_persist)

            leader_commit = int(payload.get("leader_commit", 0))
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self._last_log_index())
                self._apply_committed_locked()
            return {"term": self.current_term, "success": True}

    def handle_install_snapshot(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.condition:
            term = int(payload["term"])
            if term < self.current_term:
                return {"term": self.current_term, "success": False}
            if term > self.current_term:
                self._become_follower_locked(term)
            self.leader_id = payload["leader_id"]
            self._reset_election_deadline_locked()

            snapshot_index = int(payload["last_included_index"])
            snapshot_term = int(payload["last_included_term"])
            if snapshot_index <= self.commit_index:
                return {"term": self.current_term, "success": True}

            self.state_machine = KVStateMachine.from_snapshot(payload["state"])
            self.storage.save_snapshot(snapshot_index, snapshot_term, self.state_machine.snapshot())
            self.snapshot_index = snapshot_index
            self.snapshot_term = snapshot_term
            self.log = RaftLog(base_index=snapshot_index)
            self.commit_index = snapshot_index
            self.last_applied = snapshot_index
            self.condition.notify_all()
            return {"term": self.current_term, "success": True}

    # ------------------------------------------------------------------
    # Leader replication
    # ------------------------------------------------------------------
    def _send_heartbeats_locked(self) -> None:
        if self.state != self.LEADER:
            return
        self.last_heartbeat_sent = time.monotonic()
        for peer_id in self._peers():
            threading.Thread(target=self._heartbeat_once, args=(peer_id,), daemon=True).start()

    def _heartbeat_once(self, peer_id: str) -> None:
        with self.condition:
            if self.state != self.LEADER:
                return
            term = self.current_term
            prev_index = self._last_log_index()
            prev_term = self._term_at_global(prev_index) or 0
        request = make_raft_message(
            "append_entries",
            {
                "term": term,
                "leader_id": self.node_id,
                "prev_log_index": prev_index,
                "prev_log_term": prev_term,
                "entries": [],
                "leader_commit": self.commit_index,
            },
        )
        try:
            response = call(
                Address(*self.cluster.address(peer_id)),
                request,
                self.config.rpc_timeout,
            )["payload"]
        except RPCError:
            return
        with self.condition:
            if response.get("term", 0) > self.current_term:
                self._become_follower_locked(response["term"])

    def _ensure_replication_locked(self) -> None:
        if self.state != self.LEADER:
            return
        for peer_id in self._peers():
            if peer_id not in self._replicating:
                self._replicating.add(peer_id)
                threading.Thread(
                    target=self._replication_worker, args=(peer_id,), daemon=True
                ).start()

    def _replication_worker(self, peer_id: str) -> None:
        try:
            while not self.stop_event.is_set():
                success = self._replicate_once(peer_id)
                if success == "stop":
                    return
                if success == "done":
                    return
                time.sleep(0.005)
        finally:
            with self.condition:
                self._replicating.discard(peer_id)

    def _replicate_once(self, peer_id: str) -> str:
        with self.condition:
            if self.state != self.LEADER:
                return "stop"
            term = self.current_term
            next_index = self.next_index.get(peer_id, self._last_log_index() + 1)
            if next_index <= self.snapshot_index:
                snapshot_index = self.snapshot_index
                snapshot_term = self.snapshot_term
                snapshot_state = self.state_machine.snapshot()
            else:
                prev_index = next_index - 1
                prev_term = self._term_at_global(prev_index) or 0
                entries = [entry.to_dict() for entry in self.log.slice_from(next_index)]
                leader_commit = self.commit_index

        if next_index <= self.snapshot_index:
            request = make_raft_message(
                "install_snapshot",
                {
                    "term": term,
                    "leader_id": self.node_id,
                    "last_included_index": snapshot_index,
                    "last_included_term": snapshot_term,
                    "state": snapshot_state,
                },
            )
        else:
            request = make_raft_message(
                "append_entries",
                {
                    "term": term,
                    "leader_id": self.node_id,
                    "prev_log_index": prev_index,
                    "prev_log_term": prev_term,
                    "entries": entries,
                    "leader_commit": leader_commit,
                },
            )

        try:
            response = call(
                Address(*self.cluster.address(peer_id)),
                request,
                self.config.rpc_timeout,
            )["payload"]
        except RPCError:
            return "retry"

        with self.condition:
            if response.get("term", 0) > self.current_term:
                self._become_follower_locked(response["term"])
                return "stop"
            if self.state != self.LEADER:
                return "stop"
            if next_index <= self.snapshot_index:
                if response.get("success"):
                    self.match_index[peer_id] = max(
                        self.match_index.get(peer_id, 0), snapshot_index
                    )
                    self.next_index[peer_id] = snapshot_index + 1
                    self._commit_locked()
                    self._apply_committed_locked()
                    self.condition.notify_all()
                    return "retry"
            else:
                if response.get("success"):
                    new_match = prev_index + len(entries)
                    self.match_index[peer_id] = max(
                        self.match_index.get(peer_id, 0), new_match
                    )
                    self.next_index[peer_id] = new_match + 1
                    self._commit_locked()
                    self._apply_committed_locked()
                    self.condition.notify_all()
                    return "done"
            self.next_index[peer_id] = max(self.snapshot_index + 1, next_index - 1)
            return "retry"

    def _commit_locked(self) -> None:
        if self.state != self.LEADER:
            return
        for raft_index in range(self._last_log_index(), self.commit_index, -1):
            if self._term_at_global(raft_index) != self.current_term:
                continue
            count = 1
            for peer_id in self._peers():
                if self.match_index.get(peer_id, 0) >= raft_index:
                    count += 1
            if count >= self.cluster.quorum_size:
                self.commit_index = raft_index
                break

    def _apply_committed_locked(self) -> None:
        while self.last_applied < self.commit_index:
            next_index = self.last_applied + 1
            entry = self._entry_at_global(next_index)
            if entry is None:
                # A missing entry indicates a snapshot/replication bug; stop
                # rather than silently skipping state transitions.
                break
            self.state_machine.apply(entry.command)
            self.last_applied = entry.index
        self._maybe_snapshot_locked()
        self.condition.notify_all()

    def _maybe_snapshot_locked(self) -> None:
        if len(self.log) < self.config.snapshot_threshold:
            return
        snapshot_index = self.last_applied
        snapshot_term = self._term_at_global(snapshot_index) or 0
        self.storage.save_snapshot(
            snapshot_index, snapshot_term, self.state_machine.snapshot()
        )
        retained = self.log.slice_from(snapshot_index + 1)
        self.snapshot_index = snapshot_index
        self.snapshot_term = snapshot_term
        self.log = RaftLog(retained, base_index=snapshot_index)

    # ------------------------------------------------------------------
    # Client operations
    # ------------------------------------------------------------------
    def propose(self, command: dict[str, Any], timeout: float = 2.0) -> str | None:
        key = command.get("key", "")
        with self.condition:
            if self.state != self.LEADER:
                raise NotLeaderError(self.leader_id, self._leader_address(self.leader_id))
            entry = self.log.append_command(self.current_term, command)
            self.storage.append_entry(entry)
            self._ensure_replication_locked()
            deadline = time.monotonic() + timeout
            while (
                entry.index > self.commit_index
                and self.state == self.LEADER
                and self.current_term == entry.term
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self.condition.wait(remaining)
            if self.state != self.LEADER or self.current_term != entry.term:
                raise NotLeaderError(self.leader_id, self._leader_address(self.leader_id))
            if entry.index > self.commit_index:
                raise ProposalTimeout(
                    f"command at index {entry.index} was not committed within {timeout:.1f}s"
                )
            self._apply_committed_locked()
            if command.get("op") == "delete":
                return None
            return self.state_machine.get(str(key))

    def linearizable_read(self, key: str) -> str | None:
        with self.condition:
            if self.state != self.LEADER:
                raise NotLeaderError(self.leader_id, self._leader_address(self.leader_id))
        if not self._quorum_ack():
            raise NotLeaderError(self.leader_id, self._leader_address(self.leader_id))
        with self.condition:
            if self.state != self.LEADER:
                raise NotLeaderError(self.leader_id, self._leader_address(self.leader_id))
            self._apply_committed_locked()
            return self.state_machine.get(key)

    def _quorum_ack(self) -> bool:
        """Confirm leadership with a round of heartbeats before a read."""
        with self.condition:
            if self.state != self.LEADER:
                return False
            term = self.current_term
            prev_index = self._last_log_index()
            prev_term = self._term_at_global(prev_index) or 0
        peers = self._peers()
        successes: list[bool] = []

        def ask(peer_id: str) -> None:
            request = make_raft_message(
                "append_entries",
                {
                    "term": term,
                    "leader_id": self.node_id,
                    "prev_log_index": prev_index,
                    "prev_log_term": prev_term,
                    "entries": [],
                    "leader_commit": self.commit_index,
                },
            )
            try:
                response = call(
                    Address(*self.cluster.address(peer_id)),
                    request,
                    self.config.rpc_timeout,
                )["payload"]
                successes.append(
                    response.get("term", 0) == term and response.get("success", False)
                )
            except RPCError:
                successes.append(False)

        if peers:
            with ThreadPoolExecutor(max_workers=len(peers)) as executor:
                list(executor.map(lambda peer_id: ask(peer_id), peers))
        return (sum(successes) + 1) >= self.cluster.quorum_size

    def _leader_address(self, leader_id: str | None) -> tuple[str, int] | None:
        if leader_id and leader_id in self.cluster.nodes:
            return self.cluster.address(leader_id)
        return None

    def status(self) -> dict[str, Any]:
        with self.condition:
            return {
                "id": self.node_id,
                "state": self.state,
                "term": self.current_term,
                "leader_id": self.leader_id,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "last_log_index": self._last_log_index(),
                "log_size": len(self.log),
                "snapshot_index": self.snapshot_index,
                "peers": {
                    peer_id: {
                        "next_index": self.next_index.get(peer_id),
                        "match_index": self.match_index.get(peer_id, 0),
                    }
                    for peer_id in self._peers()
                },
            }

    # ------------------------------------------------------------------
    # Main timer loop
    # ------------------------------------------------------------------
    def run(self) -> None:
        while not self.stop_event.is_set():
            with self.condition:
                now = time.monotonic()
                if self.state == self.LEADER:
                    if now - self.last_heartbeat_sent >= self.config.heartbeat_interval:
                        self._send_heartbeats_locked()
                elif now >= self.election_deadline:
                    self._start_election_locked()
            self.stop_event.wait(0.01)

    def start(self) -> None:
        self.stop_event.clear()
        super().start()

    def stop(self) -> None:
        self.stop_event.set()
        with self.condition:
            self.condition.notify_all()
