"""The shared audit log (docs/conventions.md, "Audit").

Write with the driver you already hold, inside the change's own transaction::

    from vex_platform.audit import AuditEntry, target
    from vex_platform.audit.psycopg import record          # or .sqlalchemy

    await record(conn, AuditEntry("vod.update", actor, target("vod", vod_id), before=old, after=new))
"""

from ..actor import SYSTEM, Actor
from .model import OUTCOMES, AuditEntry, check_action, target

__all__ = ["OUTCOMES", "SYSTEM", "Actor", "AuditEntry", "check_action", "target"]
