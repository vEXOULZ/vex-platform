"""X-Request-ID: taken from the request when a proxy set a sane one, else generated. It is echoed on the
response, bound into structlog's contextvars for every log line of the request, and kept on
``request.state.request_id`` for audit rows."""

from __future__ import annotations

import re
import uuid

import structlog
from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

HEADER = b"x-request-id"
_SANE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


def request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


class RequestIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        incoming = dict(scope.get("headers") or []).get(HEADER, b"").decode("latin-1")
        rid = incoming if _SANE.match(incoming) else uuid.uuid4().hex
        scope.setdefault("state", {})["request_id"] = rid

        async def send_with_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = [(k, v) for k, v in message.get("headers", []) if k.lower() != HEADER]
                headers.append((HEADER, rid.encode()))
                message["headers"] = headers
            await send(message)

        with structlog.contextvars.bound_contextvars(request_id=rid):
            await self.app(scope, receive, send_with_id)
