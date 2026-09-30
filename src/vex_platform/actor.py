"""Who did something: shared by the API (request.state.actor), audit rows and job runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal

ActorKind = Literal["user", "api_key", "system", "job", "anonymous"]
Via = Literal["api", "web", "chat", "cli", "job", "system"]

ACTOR_KINDS: tuple[str, ...] = ("user", "api_key", "system", "job", "anonymous")
VIAS: tuple[str, ...] = ("api", "web", "chat", "cli", "job", "system")


@dataclass(frozen=True, slots=True)
class Actor:
    """``kind`` is what authenticated, ``id`` who within that kind (a Twitch user id, an API key's
    label), ``login`` a readable name when there is one, ``via`` the surface the action came through."""

    kind: ActorKind
    id: str | None = None
    login: str | None = None
    via: Via = "system"

    def __post_init__(self) -> None:
        if self.kind not in ACTOR_KINDS:
            raise ValueError(f"unknown actor kind {self.kind!r}")
        if self.via not in VIAS:
            raise ValueError(f"unknown via {self.via!r}")

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> Actor:
        return cls(**value) if value else SYSTEM

    def label(self) -> str:
        """For log lines: ``user:1234 (vex)``."""
        who = f"{self.kind}:{self.id}" if self.id else self.kind
        return f"{who} ({self.login})" if self.login else who


SYSTEM = Actor("system", via="system")
