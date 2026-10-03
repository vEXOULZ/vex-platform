"""Driver-neutral SQL for the audit table: the psycopg and SQLAlchemy writers share it."""

from __future__ import annotations

import json
from typing import Any

from .model import AuditEntry

COLUMNS = (
    "at",
    "actor_kind",
    "actor_id",
    "actor_login",
    "via",
    "action",
    "target",
    "scope",
    "outcome",
    "before",
    "after",
    "detail",
    "request_id",
    "job_run_id",
)
SELECT_COLUMNS = ("id", *COLUMNS)


def _json(value: Any) -> str | None:
    return None if value is None else json.dumps(value, default=str, separators=(",", ":"))


def values(entry: AuditEntry) -> dict[str, Any]:
    """Named parameters for ``insert_sql``; JSON columns already encoded (both drivers take text)."""
    return {
        "at": entry.at,
        "actor_kind": entry.actor.kind,
        "actor_id": entry.actor.id,
        "actor_login": entry.actor.login,
        "via": entry.actor.via,
        "action": entry.action,
        "target": entry.target,
        "scope": entry.scope,
        "outcome": entry.outcome,
        "before": _json(entry.before),
        "after": _json(entry.after),
        "detail": _json(entry.detail),
        "request_id": entry.request_id,
        "job_run_id": entry.job_run_id,
    }


def insert_sql(table: str, style: str) -> str:
    """``style`` "pyformat" (psycopg: ``%(name)s``) or "named" (SQLAlchemy text: ``:name``)."""

    def mark(c: str) -> str:
        return f"%({c})s" if style == "pyformat" else f":{c}"

    placeholders = []
    for col in COLUMNS:
        p = mark(col)
        if col == "at":
            p = f"COALESCE({p}, now())"
        elif col in ("before", "after", "detail"):
            p = f"CAST({p} AS jsonb)"
        placeholders.append(p)
    return f"INSERT INTO {table} ({', '.join(COLUMNS)}) VALUES ({', '.join(placeholders)}) RETURNING id"
