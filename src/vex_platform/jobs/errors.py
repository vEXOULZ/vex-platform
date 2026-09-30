"""Exceptions of the jobs layer: the first three for callers, the rest for step code."""

from __future__ import annotations


class JobNotFound(LookupError):
    def __init__(self, run_id: int) -> None:
        super().__init__(f"no job run {run_id}")
        self.run_id = run_id


class JobConflict(Exception):
    """The run is in a state that does not allow the action; the message says why."""


class InvalidJob(ValueError):
    """An unknown job kind or step name."""


class StepError(RuntimeError):
    """An expected failure with a readable message: stored as ``last_error`` without a traceback,
    and retried like any other failure."""


class StepRefused(StepError):
    """A step that must not run for this subject: the run fails at once instead of retrying."""


class RunStopped(Exception):
    """Raised by ``StepContext.check_stop()`` when the run was cancelled or the worker is shutting
    down. The step is not finished: a shutdown re-queues the run at this step, a cancel ends it."""
