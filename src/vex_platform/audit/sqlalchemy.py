"""Audit writes over a SQLAlchemy (async) session or connection (twitch-archive)."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from .model import AuditEntry
from .sql import insert_sql, values


async def record(session: Any, entry: AuditEntry, *, table: str = "audit_log") -> int:
    """Insert one row through ``session`` (an ``AsyncSession`` or ``AsyncConnection``): call it before
    the commit of the change it records, so both land together or not at all."""
    result = await session.execute(text(insert_sql(table, "named")), values(entry))
    return int(result.scalar_one())
