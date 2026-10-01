from __future__ import annotations

import psycopg
import pytest
from conftest import AUDIT, DSN, audit_rows
from sqlalchemy.ext.asyncio import create_async_engine

from vex_platform import migrations
from vex_platform.audit import SYSTEM, Actor, AuditEntry, target
from vex_platform.audit import psycopg as audit_pg
from vex_platform.audit import sqlalchemy as audit_sa

pytestmark = pytest.mark.usefixtures("dsn")

VEX = Actor("user", "1", "vex", "web")


def test_entry_validation():
    assert target("vod", 12) == "vod:12"
    AuditEntry("vod.chapters.replace")
    for bad in ("vod", "Vod.update", "PATCH /admin/vods", "vod..x"):
        with pytest.raises(ValueError):
            AuditEntry(bad)
    with pytest.raises(ValueError):
        AuditEntry("vod.update", outcome="maybe")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Actor("robot")  # type: ignore[arg-type]
    assert VEX.label() == "user:1 (vex)" and SYSTEM.label() == "system"
    assert Actor.from_dict(VEX.as_dict()) == VEX and Actor.from_dict(None) == SYSTEM


async def test_psycopg_record_joins_the_transaction():
    async with await psycopg.AsyncConnection.connect(DSN) as conn:
        await audit_pg.record(conn, AuditEntry("vod.update", VEX, "vod:1"), table=AUDIT)
        await conn.rollback()
        row_id = await audit_pg.record(
            conn,
            AuditEntry("vod.update", VEX, "vod:1", scope="456", before={"title": "a"}, after={"title": "b"},
                       request_id="r1"),
            table=AUDIT,
        )
        await conn.commit()
    [row] = await audit_rows()
    assert row["id"] == row_id
    assert (row["actor_kind"], row["actor_id"], row["actor_login"], row["via"]) == ("user", "1", "vex", "web")
    assert (row["before"], row["after"], row["outcome"], row["request_id"]) == (
        {"title": "a"}, {"title": "b"}, "ok", "r1")
    assert row["at"] is not None


async def test_sqlalchemy_record_joins_the_transaction():
    engine = create_async_engine(DSN.replace("postgresql://", "postgresql+psycopg://"))
    try:
        async with engine.connect() as conn:
            await audit_sa.record(conn, AuditEntry("storage.delete", target=target("file", "x")), table=AUDIT)
            await conn.rollback()
            await audit_sa.record(conn, AuditEntry("storage.delete", detail={"bytes": 10}), table=AUDIT)
            await conn.commit()
    finally:
        await engine.dispose()
    [row] = await audit_rows()
    assert (row["action"], row["detail"], row["actor_kind"]) == ("storage.delete", {"bytes": 10}, "system")


async def test_read_filters():
    entries = [
        AuditEntry("vod.update", VEX, "vod:1", scope="a"),
        AuditEntry("vod.hide", VEX, "vod:2", scope="b"),
        AuditEntry("cc.create", SYSTEM, "command:x", scope="a", outcome="denied"),
        AuditEntry("vodka.drink", VEX, "vodx:1"),
    ]
    async with await psycopg.AsyncConnection.connect(DSN) as conn:
        for e in entries:
            await audit_pg.record(conn, e, table=AUDIT)
        await conn.commit()

        async def actions(**filters) -> list[str]:
            return [r["action"] for r in await audit_pg.read(conn, table=AUDIT, **filters)]

        assert await actions() == ["vodka.drink", "cc.create", "vod.hide", "vod.update"]  # newest first
        assert await actions(action="vod.") == ["vod.hide", "vod.update"]
        assert await actions(action="vod.hide") == ["vod.hide"]
        assert await actions(target="vod:") == ["vod.hide", "vod.update"]
        assert await actions(scopes=["a"]) == ["cc.create", "vod.update"]
        assert await actions(outcome="denied", actor_kind="system") == ["cc.create"]
        assert await actions(actor_id="1", limit=2) == ["vodka.drink", "vod.hide"]
        rows = await audit_pg.read(conn, table=AUDIT, limit=2)
        assert await actions(before_id=rows[-1]["id"]) == ["vod.hide", "vod.update"]
        assert await actions(action="v_d.") == []  # LIKE wildcards are escaped
        assert await actions(scopes=["b"], own=("system", "x")) == ["vod.hide"]
        assert await actions(scopes=[], own=("user", "1")) == ["vodka.drink", "vod.hide", "vod.update"]
        assert await actions(actor=(None, "VEX")) == ["vodka.drink", "vod.hide", "vod.update"]  # any case
        assert await actions(actor=(("system", "x"), "nobody")) == []


@pytest.mark.parametrize("driver", ["psycopg", "asyncpg"])
async def test_migration_apply_through_sqlalchemy(driver):
    """What an Alembic revision does: ``apply`` in a transaction, into another schema and table,
    sync over psycopg and through ``run_sync`` over asyncpg (twitch-archive's migrations)."""
    url = DSN.replace("postgresql://", f"postgresql+{driver}://")
    engine = create_async_engine(url)

    def migrate(conn):
        conn.exec_driver_sql("DROP SCHEMA IF EXISTS mig_test CASCADE")
        conn.exec_driver_sql("DROP SCHEMA IF EXISTS mig_test_jobs CASCADE")
        conn.exec_driver_sql("CREATE SCHEMA mig_test")
        conn.exec_driver_sql("SET search_path TO mig_test, public")
        migrations.apply(conn, migrations.jobs_sql(1, schema="mig_test_jobs"))
        migrations.apply(conn, migrations.audit_sql(1, table="mig_test.audit_log"))
        path = conn.exec_driver_sql("SHOW search_path").scalar_one()
        tables = conn.exec_driver_sql(
            "SELECT table_schema || '.' || table_name FROM information_schema.tables"
            " WHERE left(table_schema, 8) = 'mig_test' ORDER BY 1"
        ).scalars().all()
        conn.exec_driver_sql("DROP SCHEMA mig_test CASCADE")
        conn.exec_driver_sql("DROP SCHEMA mig_test_jobs CASCADE")
        return path, tables

    try:
        async with engine.begin() as conn:
            path, tables = await conn.run_sync(migrate)
    finally:
        await engine.dispose()
    assert path == "mig_test, public"  # restored after the jobs SQL
    assert "mig_test.audit_log" in tables
    assert {"mig_test_jobs.job_runs", "mig_test_jobs.job_run_events", "mig_test_jobs.procrastinate_jobs"} <= set(tables)
    assert not any(t.startswith("mig_test.procrastinate") for t in tables)


def test_identifiers_are_checked():
    with pytest.raises(ValueError):
        migrations.jobs_sql(1, schema="jobs; drop table x")
    with pytest.raises(ValueError):
        migrations.audit_sql(1, table="a.b.c")
    with pytest.raises(ValueError):
        migrations.audit_sql(1, table="Audit")
