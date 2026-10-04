"""Deterministic key-value state machine."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class CommandError(ValueError):
    """Raised when a state-machine command is malformed."""


@dataclass
class KVStateMachine:
    """A trivial but deterministic KV store.

    Keeping the state machine separate from Raft makes it easy to swap in a
    different replicated application later (e.g. a counter or a queue).
    """

    data: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_snapshot(cls, raw: dict[str, str]) -> "KVStateMachine":
        return cls(data=dict(raw))

    def apply(self, command: dict[str, Any]) -> str | None:
        op = command.get("op")
        if op == "noop":
            return None
        key = command.get("key")
        if op == "put":
            if not isinstance(key, str) or not key:
                raise CommandError("put requires a non-empty string key")
            value = command.get("value", "")
            self.data[key] = str(value)
            return str(value)
        if op == "delete":
            if not isinstance(key, str) or not key:
                raise CommandError("delete requires a non-empty string key")
            return self.data.pop(key, None)
        raise CommandError(f"unknown command op: {op!r}")

    def get(self, key: str) -> str | None:
        return self.data.get(key)

    def items(self) -> list[tuple[str, str]]:
        return sorted(self.data.items())

    def snapshot(self) -> dict[str, str]:
        return dict(self.data)
