"""Raft replicated log primitives.

Indexes are 1-based and match the terminology in the original Raft paper.
After a snapshot the log may have an empty in-memory prefix; ``base_index`` is
the last included snapshot index, so ``last_index`` remains meaningful.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Iterator, MutableSequence


@dataclass(frozen=True)
class LogEntry:
    term: int
    index: int
    command: dict[str, Any]

    def to_dict(self) -> dict:
        return {"term": self.term, "index": self.index, "command": self.command}

    @classmethod
    def from_dict(cls, raw: dict) -> "LogEntry":
        return cls(term=int(raw["term"]), index=int(raw["index"]), command=raw["command"])


class RaftLog:
    """In-memory append-only log with Raft conflict truncation helpers."""

    def __init__(self, entries: Iterable[LogEntry] = (), base_index: int = 0) -> None:
        self.base_index = int(base_index)
        self._entries: list[LogEntry] = []
        for entry in entries:
            self.append(entry)

    def append(self, entry: LogEntry) -> None:
        if entry.index != self.last_index + 1:
            raise ValueError(
                f"log must be append-only: expected index {self.last_index + 1}, got {entry.index}"
            )
        self._entries.append(entry)

    def append_command(self, term: int, command: dict) -> LogEntry:
        entry = LogEntry(term=term, index=self.last_index + 1, command=command)
        self._entries.append(entry)
        return entry

    @property
    def entries(self) -> MutableSequence[LogEntry]:
        return self._entries

    def __len__(self) -> int:
        return len(self._entries)

    def __iter__(self) -> Iterator[LogEntry]:
        return iter(self._entries)

    def __getitem__(self, index: int) -> LogEntry:
        """Access by zero-based Python list index."""
        return self._entries[index]

    @property
    def last_index(self) -> int:
        return self._entries[-1].index if self._entries else self.base_index

    @property
    def last_term(self) -> int:
        return self._entries[-1].term if self._entries else 0

    def term_at(self, raft_index: int) -> int | None:
        """Return the term at a 1-based Raft index, or ``None`` if absent."""
        if raft_index <= self.base_index:
            return None
        local_index = raft_index - self.base_index - 1
        if local_index >= len(self._entries):
            return None
        return self._entries[local_index].term

    def entry_at(self, raft_index: int) -> LogEntry | None:
        if raft_index <= self.base_index:
            return None
        local_index = raft_index - self.base_index - 1
        if local_index >= len(self._entries):
            return None
        return self._entries[local_index]

    def slice_from(self, raft_index: int) -> list[LogEntry]:
        """Return entries starting at a 1-based Raft index."""
        if raft_index <= self.base_index + 1:
            return list(self._entries)
        local_index = raft_index - self.base_index - 1
        return list(self._entries[local_index:])

    def truncate_from(self, raft_index: int) -> list[LogEntry]:
        """Delete entries from a 1-based Raft index onward."""
        if raft_index <= self.base_index + 1:
            removed = list(self._entries)
            self._entries.clear()
            return removed
        local_index = raft_index - self.base_index - 1
        removed = list(self._entries[local_index:])
        del self._entries[local_index:]
        return removed

    def replace_from(self, raft_index: int, entries: Iterable[LogEntry]) -> None:
        """Truncate conflicts and append new entries, used by followers."""
        self.truncate_from(raft_index)
        for entry in entries:
            if entry.index != self.last_index + 1:
                self.truncate_from(entry.index)
            self.append(entry)
