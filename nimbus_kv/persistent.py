"""Durable Raft state: metadata, WAL, and snapshots."""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .log import LogEntry


@dataclass
class LoadedState:
    current_term: int
    voted_for: str | None
    entries: list[LogEntry]
    snapshot_index: int
    snapshot_term: int
    state: dict[str, str]


class RaftStorage:
    """Append-only JSONL WAL plus JSON snapshot files.

    Every metadata update is written with a rename-based atomic swap. The WAL
    is flushed after each append so a crash cannot silently lose acknowledged
    entries.
    """

    def __init__(self, data_dir: str | Path, snapshot_threshold: int = 500) -> None:
        self.data_dir = Path(data_dir)
        self.snapshot_threshold = snapshot_threshold
        self.meta_path = self.data_dir / "meta.json"
        self.wal_path = self.data_dir / "wal.jsonl"
        self.snapshot_path = self.data_dir / "snapshot.json"
        self.data_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _atomic_write(path: Path, data: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(data, handle, ensure_ascii=False, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, path)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def load(self) -> LoadedState:
        meta = {"current_term": 0, "voted_for": None}
        if self.meta_path.exists():
            meta = json.loads(self.meta_path.read_text(encoding="utf-8"))

        snapshot_index = 0
        snapshot_term = 0
        state: dict[str, str] = {}
        if self.snapshot_path.exists():
            snapshot = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
            snapshot_index = int(snapshot["last_included_index"])
            snapshot_term = int(snapshot["last_included_term"])
            state = dict(snapshot.get("state", {}))

        entries: list[LogEntry] = []
        if self.wal_path.exists():
            with self.wal_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    entries.append(LogEntry.from_dict(json.loads(line)))

        return LoadedState(
            current_term=int(meta.get("current_term", 0)),
            voted_for=meta.get("voted_for"),
            entries=entries,
            snapshot_index=snapshot_index,
            snapshot_term=snapshot_term,
            state=state,
        )

    def save_meta(self, current_term: int, voted_for: str | None) -> None:
        self._atomic_write(
            self.meta_path, {"current_term": current_term, "voted_for": voted_for}
        )

    def append_entry(self, entry: LogEntry) -> None:
        with self.wal_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry.to_dict(), ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

    def append_entries(self, entries: list[LogEntry]) -> None:
        if not entries:
            return
        with self.wal_path.open("a", encoding="utf-8") as handle:
            for entry in entries:
                handle.write(
                    json.dumps(entry.to_dict(), ensure_ascii=False, separators=(",", ":"))
                )
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

    def truncate_wal_from(self, raft_index: int) -> None:
        """Remove WAL entries from ``raft_index`` onward; used after conflict."""
        entries = []
        if self.wal_path.exists():
            with self.wal_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    entry = LogEntry.from_dict(json.loads(line))
                    if entry.index < raft_index:
                        entries.append(entry)
        self._rewrite_wal(entries)

    def _rewrite_wal(self, entries: list[LogEntry]) -> None:
        fd, tmp_name = tempfile.mkstemp(dir=self.data_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                for entry in entries:
                    handle.write(
                        json.dumps(entry.to_dict(), ensure_ascii=False, separators=(",", ":"))
                    )
                    handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self.wal_path)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def save_snapshot(
        self, last_included_index: int, last_included_term: int, state: dict[str, str]
    ) -> None:
        self._atomic_write(
            self.snapshot_path,
            {
                "last_included_index": last_included_index,
                "last_included_term": last_included_term,
                "state": state,
            },
        )
        self._rewrite_wal([])
