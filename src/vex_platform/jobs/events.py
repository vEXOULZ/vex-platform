"""Per-run event log: every ``ctx.log`` line, step change and progress report (``job_run_events``).

Events are buffered in memory and written in batches (``run_forever`` flushes about once a second),
so logging never waits on the database. Each run keeps roughly its newest ``cap`` events; older ones
are pruned as new ones arrive. ``add`` is thread-safe: progress may arrive from a worker thread.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from collections import defaultdict, deque
from typing import Any, Literal

import structlog
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

log = structlog.get_logger(__name__)

Level = Literal["info", "warning", "error"]
LEVELS = ("info", "warning", "error")
UNITS = ("items", "parts", "bytes", "percent", "seconds")
MAX_PER_RUN = 1000
PRUNE_EVERY = 100  # events per run between prunes

_PRUNE = """
DELETE FROM job_run_events WHERE run_id = %(run_id)s AND id < (
    SELECT id FROM job_run_events WHERE run_id = %(run_id)s ORDER BY id DESC OFFSET %(keep)s LIMIT 1
)
"""


def progress(done: float, total: float | None = None, unit: str = "items") -> dict[str, Any]:
    if unit not in UNITS:
        raise ValueError(f"unknown progress unit {unit!r}; units: {', '.join(UNITS)}")
    return {"done": done, "total": total, "unit": unit}


class RunEvents:
    def __init__(self, pool: AsyncConnectionPool, cap: int = MAX_PER_RUN) -> None:
        self.pool = pool
        self.cap = cap
        self._pending: deque[dict[str, Any]] = deque(maxlen=50_000)  # bounded if the database is down
        self._since_prune: defaultdict[int, int] = defaultdict(int)
        self._lock = asyncio.Lock()

    def add(
        self,
        run_id: int,
        level: Level,
        step: str | None,
        message: str,
        progress: dict[str, Any] | None = None,
        *,
        at: dt.datetime | None = None,
    ) -> None:
        # deque.append is atomic, so a thread may call this.
        self._pending.append({
            "run_id": run_id,
            "at": at or dt.datetime.now(dt.UTC),
            "level": level if level in LEVELS else "info",
            "step": step,
            "message": message,
            "progress": Jsonb(progress) if progress is not None else None,
        })

    async def flush(self) -> None:
        async with self._lock:
            rows = []
            while self._pending:
                rows.append(self._pending.popleft())
            if not rows:
                return
            try:
                async with self.pool.connection() as conn, conn.cursor() as cur:
                    await cur.executemany(
                        "INSERT INTO job_run_events (run_id, at, level, step, message, progress)"
                        " VALUES (%(run_id)s, %(at)s, %(level)s, %(step)s, %(message)s, %(progress)s)",
                        rows,
                    )
                    for row in rows:
                        self._since_prune[row["run_id"]] += 1
                    for run_id in [r for r, n in self._since_prune.items() if n >= PRUNE_EVERY]:
                        await cur.execute(_PRUNE, {"run_id": run_id, "keep": self.cap - 1})
                        del self._since_prune[run_id]
            except Exception as exc:
                # Events of a run whose row was deleted fail the whole batch: drop them, keep going.
                log.warning("jobs.events_dropped", count=len(rows), error=str(exc))

    async def run_forever(self, interval: float = 1.0) -> None:
        try:
            while True:
                await asyncio.sleep(interval)
                await self.flush()
        finally:
            await asyncio.shield(self.flush())

    async def list(self, run_id: int, *, after: int = 0, limit: int = 200) -> list[dict[str, Any]]:
        """Events with id > ``after``, oldest first; flushes first so a line logged a moment ago shows."""
        await self.flush()
        async with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, at, level, step, message, progress FROM job_run_events"
                " WHERE run_id = %s AND id > %s ORDER BY id LIMIT %s",
                (run_id, after, limit),
            )
            return list(await cur.fetchall())
