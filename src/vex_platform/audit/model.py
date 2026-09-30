"""One audit row, as code builds it and as the API returns it."""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass, field
from typing import Any, Literal

from ..actor import SYSTEM, Actor

Outcome = Literal["ok", "denied", "failed"]
OUTCOMES = ("ok", "denied", "failed")

# noun.verb, nouns may nest: vod.update, vod.chapters.replace, cc.create
_ACTION = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$")


def check_action(action: str) -> str:
    if not _ACTION.match(action):
        raise ValueError(f"audit action {action!r} must be dotted lowercase noun.verb")
    return action


def target(kind: str, ident: object) -> str:
    """``target("vod", 123)`` -> ``"vod:123"``."""
    return f"{kind}:{ident}"


@dataclass(frozen=True, slots=True)
class AuditEntry:
    action: str
    actor: Actor = SYSTEM
    target: str | None = None
    scope: str | None = None
    outcome: Outcome = "ok"
    before: Any = None
    after: Any = None
    detail: Any = None
    request_id: str | None = None
    job_run_id: int | None = None
    at: dt.datetime | None = field(default=None)  # None: the database's now()

    def __post_init__(self) -> None:
        check_action(self.action)
        if self.outcome not in OUTCOMES:
            raise ValueError(f"unknown outcome {self.outcome!r}")
