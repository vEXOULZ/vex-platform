"""Job kinds and the step functions they are made of.

A kind is an ordered list of step names; a step is an ``async def step(ctx)`` registered once and
shared by every kind that lists it::

    registry = Registry()

    @registry.step("fetch")
    async def fetch(ctx): ...

    registry.kind("archive", ["fetch", "upload"], lock=lambda run: run.subject)
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from .errors import InvalidJob

if TYPE_CHECKING:
    from .run import JobRun

Step = Callable[[Any], Awaitable[None]]
CancelMode = Literal["interrupt", "cooperative"]


@dataclass(frozen=True)
class JobKind:
    """
    ``lock(run)``: runs whose lock strings are equal never run at the same time (procrastinate's
    ``lock``); None for no lock. ``max_attempts``: None takes the runtime's default.
    ``cancel_mode``: "interrupt" cancels a running step at once (asyncio cancellation, like a
    shutdown); "cooperative" only flags the run, and the step stops at its next
    ``ctx.check_stop()`` — for steps that must not be interrupted mid-write.
    ``pause_before``: steps runs of this kind pause before by default (a run's own list overrides).
    """

    name: str
    steps: tuple[str, ...]
    description: str = ""
    lock: Callable[[JobRun], str | None] | None = None
    max_attempts: int | None = None
    retry_base_seconds: float = 60.0
    cancel_mode: CancelMode = "interrupt"
    pause_before: tuple[str, ...] = ()
    queue: str = "default"
    priority: int = 0

    def retry_delay(self, attempts: int) -> float:
        """Seconds before retry number ``attempts`` (1-based): base, 2x, 4x, ..."""
        return self.retry_base_seconds * 2 ** max(attempts - 1, 0)


@dataclass
class Registry:
    kinds: dict[str, JobKind] = field(default_factory=dict)
    steps: dict[str, Step] = field(default_factory=dict)

    def add_step(self, name: str, fn: Step) -> Step:
        if name in self.steps and self.steps[name] is not fn:
            raise ValueError(f"step {name!r} is already registered")
        self.steps[name] = fn
        return fn

    def step(self, name: str | None = None) -> Callable[[Step], Step]:
        def register(fn: Step) -> Step:
            return self.add_step(name or fn.__name__, fn)
        return register

    def kind(self, name: str, steps: Sequence[str], **options: Any) -> JobKind:
        if name in self.kinds:
            raise ValueError(f"job kind {name!r} is already registered")
        if not steps:
            raise ValueError(f"job kind {name!r} has no steps")
        kind = JobKind(name, tuple(steps), **options)
        self.kinds[name] = kind
        return kind

    def get(self, name: str) -> JobKind:
        try:
            return self.kinds[name]
        except KeyError:
            raise InvalidJob(f"unknown job kind {name!r}; kinds: {', '.join(self.kinds)}") from None

    def check_steps(self, kind: str, names: Iterable[str]) -> None:
        """InvalidJob unless ``kind`` exists and every name is one of its steps."""
        steps = self.get(kind).steps
        unknown = [s for s in names if s not in steps]
        if unknown:
            raise InvalidJob(f"{kind!r} has no step(s) {', '.join(unknown)}; steps: {', '.join(steps)}")

    def validate(self) -> None:
        """Every kind's steps are registered: call at startup to fail on a typo there, not mid-run."""
        for kind in self.kinds.values():
            missing = [s for s in kind.steps if s not in self.steps]
            if missing:
                raise InvalidJob(f"job kind {kind.name!r} lists unregistered step(s) {', '.join(missing)}")
            self.check_steps(kind.name, kind.pause_before)
