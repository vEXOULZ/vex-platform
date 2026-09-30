"""Base for v2 request and response bodies: snake_case fields, datetimes as ISO 8601 UTC ending in Z."""

from __future__ import annotations

import datetime as dt
from typing import Annotated

from pydantic import BaseModel, ConfigDict, PlainSerializer


def utc_iso(value: dt.datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


UtcDatetime = Annotated[dt.datetime, PlainSerializer(utc_iso, return_type=str)]


class ApiModel(BaseModel):
    """Unknown fields in a request are an error rather than silently ignored."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
