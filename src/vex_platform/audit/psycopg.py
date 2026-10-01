"""Audit writes and reads over psycopg 3 (doomtp-bot, and the jobs layer's own pool)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from .model import AuditEntry
from .sql import SELECT_COLUMNS, insert_sql, values


async def record(conn: AsyncConnection[Any], entry: AuditEntry, *, table: str = "audit_log") -> int:
    """Insert one row on ``conn``: call it inside the transaction that makes the change, so the row
    exists exactly when the change does."""
    async with conn.cursor() as cur:
        await cur.execute(insert_sql(table, "pyformat"), values(entry))
        row = await cur.fetchone()
        assert row is not None
        return int(row[0] if not isinstance(row, dict) else row["id"])


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")


async def read(
    conn: AsyncConnection[Any],
    *,
    table: str = "audit_log",
    limit: int = 50,
    before_id: int | None = None,
    actor_kind: str | None = None,
    actor_id: str | None = None,
    action: str | None = None,
    target: str | None = None,
    scope: str | None = None,
    scopes: Sequence[str] | None = None,
    outcome: str | None = None,
    own: tuple[str, str] | None = None,
    actor: tuple[tuple[str, str] | None, str] | None = None,
) -> list[dict[str, Any]]:
    """Newest first. ``action`` matches exactly or, ending in ``.``, every action under it (``vod.``).
    ``target`` ending in ``:`` matches every target of that type (``vod:``). ``scopes`` limits to some
    channels (a caller who may only see their own); ``own``, an ``(actor_kind, actor_id)``, keeps that
    actor's rows visible outside them. ``actor`` is ``(kind and id or None, login)``: rows by that actor,
    or whose ``actor_login`` is the login (any case)."""
    clauses: list[str] = []
    params: list[Any] = []

    def add(sql: str, value: Any) -> None:
        clauses.append(sql)
        params.append(value)

    if before_id is not None:
        add("id < %s", before_id)
    if actor_kind is not None:
        add("actor_kind = %s", actor_kind)
    if actor_id is not None:
        add("actor_id = %s", actor_id)
    if action is not None:
        if action.endswith("."):
            add("action LIKE %s", _escape_like(action) + "%")
        else:
            add("action = %s", action)
    if target is not None:
        if target.endswith(":"):
            add("target LIKE %s", _escape_like(target) + "%")
        else:
            add("target = %s", target)
    if scope is not None:
        add("scope = %s", scope)
    if scopes is not None:
        if own is None:
            add("scope = ANY(%s)", list(scopes))
        else:
            clauses.append("(scope = ANY(%s) OR (actor_kind = %s AND actor_id = %s))")
            params.extend([list(scopes), *own])
    if actor is not None:
        who, login = actor
        if who is None:
            add("lower(actor_login) = lower(%s)", login)
        else:
            clauses.append("((actor_kind = %s AND actor_id = %s) OR lower(actor_login) = lower(%s))")
            params.extend([*who, login])
    if outcome is not None:
        add("outcome = %s", outcome)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {', '.join(SELECT_COLUMNS)} FROM {table}{where} ORDER BY id DESC LIMIT %s",
            (*params, limit),
        )
        return list(await cur.fetchall())
