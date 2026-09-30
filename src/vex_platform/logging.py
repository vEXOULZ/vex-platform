"""structlog setup: JSON to stdout in containers, pretty console locally.

Stdlib loggers (uvicorn, libraries, code not yet on structlog) go through the same renderer, so a
container's output is one JSON object per line whichever API logged it. Ported from doomtp-bot's log.py.
"""

from __future__ import annotations

import logging
import re
import sys
from typing import Literal

import structlog

_SENSITIVE_QUERY = re.compile(r"([?&](?:code|state|access_token|refresh_token|token|key)=)[^&\s\"]+")


class RedactQueryFilter(logging.Filter):
    """Mask OAuth codes and tokens in logged URLs (uvicorn access log, library messages)."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.args, tuple):
            record.args = tuple(redact(a) for a in record.args)
        elif isinstance(record.args, dict):  # "%(name)s"-style args must stay a mapping
            record.args = {k: redact(v) for k, v in record.args.items()}
        record.msg = redact(record.msg)
        return True


def redact(value: object) -> object:
    return _SENSITIVE_QUERY.sub(r"\1[redacted]", value) if isinstance(value, str) else value


def configure_logging(level: str = "INFO", fmt: Literal["json", "console"] = "console") -> None:
    """Call once at startup. ``fmt="json"`` in containers."""
    level = level.upper()
    shared: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer() if fmt == "json" else structlog.dev.ConsoleRenderer()
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(RedactQueryFilter())
    handler.setFormatter(structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[*shared, structlog.stdlib.ExtraAdder()],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
    ))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        cache_logger_on_first_use=True,
    )
