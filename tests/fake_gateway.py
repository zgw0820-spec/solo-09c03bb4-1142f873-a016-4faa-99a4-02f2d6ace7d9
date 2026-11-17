"""In-process fake notification gateway with failure injection."""

from __future__ import annotations

import asyncio
import json
import math
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Response, status


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _contact_key(contact: Any) -> str:
    """Same bucketing rule as the real gateway: channel + address, else name."""
    if isinstance(contact, str):
        try:
            contact = json.loads(contact)
        except ValueError:
            return f"raw:{contact}"
    if isinstance(contact, dict):
        channel = contact.get("channel") or ""
        address = contact.get("address") or contact.get("name") or ""
        return f"{channel}:{address}"
    return f"raw:{contact}"


@dataclass
class FakeGateway:
    # Keyed by notification_no, the gateway's dedupe key.
    records: dict[str, dict[str, Any]] = field(default_factory=dict)
    deliveries: list[str] = field(default_factory=list)
    # Delay before acceptance, simulating a slow gateway.
    slow_seconds: float = 0.0
    # Accept, persist, but then make the caller wait past its timeout and drop
    # the reply, so the caller's receipt is lost.
    drop_receipt: bool = False
    drop_hold_seconds: float = 0.4
    reject: bool = False
    # When true, the lookup endpoint is unreachable, so an ambiguous receipt
    # cannot be resolved and must stay unknown without escalating.
    lookup_down: bool = False
    # Namespace used to inject errors per role in specific tests.
    fail_roles: set[str] = field(default_factory=set)
    post_calls: int = 0
    lookups: list[str] = field(default_factory=list)
    # Per-contact rate limiting: at most ``rate_limit_max`` new notifications
    # per contact inside any sliding ``rate_limit_window_seconds`` window;
    # excess gets HTTP 429 with a retry-after hint. 0 disables limiting
    # (the default, so existing tests opt in explicitly).
    rate_limit_window_seconds: float = 0.0
    rate_limit_max: int = 1
    accept_times: dict[str, list[float]] = field(default_factory=dict)

    def rate_limit_retry_after(self, contact: Any) -> float:
        """Seconds until this contact may be accepted again; 0 when allowed."""
        if self.rate_limit_window_seconds <= 0 or self.rate_limit_max <= 0:
            return 0.0
        now = time.time()
        key = _contact_key(contact)
        recent = [
            t
            for t in self.accept_times.get(key, [])
            if now - t < self.rate_limit_window_seconds
        ]
        self.accept_times[key] = recent
        if len(recent) >= self.rate_limit_max:
            return max(0.0, self.rate_limit_window_seconds - (now - min(recent)))
        return 0.0

    def note_acceptance(self, contact: Any) -> None:
        if self.rate_limit_window_seconds <= 0:
            return
        self.accept_times.setdefault(_contact_key(contact), []).append(time.time())

    def deliver(self, payload: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        no = payload["notification_no"]
        existing = self.records.get(no)
        if existing is not None:
            existing["duplicate_requests"] += 1
            return False, existing
        record = {
            "notification_no": no,
            "alarm_no": payload["alarm_no"],
            "role": payload["role"],
            "contact": payload["contact"],
            "content_hash": payload.get("content_hash"),
            "payload": payload.get("payload", {}),
            "status": "accepted",
            "deliveries": 1,
            "duplicate_requests": 0,
            "accepted_at": _now_iso(),
        }
        self.records[no] = record
        self.deliveries.append(no)
        return True, record

    def role_notifications(self, role: str) -> list[dict[str, Any]]:
        return [r for r in self.records.values() if r["role"] == role]


def build_fake_gateway_app(fake: FakeGateway) -> FastAPI:
    app = FastAPI()

    @app.post("/notifications", status_code=status.HTTP_202_ACCEPTED)
    async def accept(payload: dict[str, Any], response: Response) -> dict[str, Any]:
        fake.post_calls += 1
        if fake.reject or payload.get("role") in fake.fail_roles:
            raise HTTPException(status_code=422, detail="rejected by fake gateway")
        # Rate limiting applies to new notification numbers only: a retry of an
        # already-accepted number falls through to the dedupe path for free.
        if payload.get("notification_no") not in fake.records:
            wait = fake.rate_limit_retry_after(payload.get("contact"))
            if wait > 0:
                response.status_code = status.HTTP_429_TOO_MANY_REQUESTS
                response.headers["Retry-After"] = str(math.ceil(wait))
                return {
                    "status": "rate_limited",
                    "notification_no": payload.get("notification_no"),
                    "retry_after_seconds": round(wait, 3),
                    "detail": "contact rate limited by fake gateway",
                }
            fake.note_acceptance(payload.get("contact"))
        if fake.slow_seconds:
            await asyncio.sleep(fake.slow_seconds)
        created, record = fake.deliver(payload)
        if fake.drop_receipt:
            # Acceptance already happened; the caller just never hears about it.
            await asyncio.sleep(fake.drop_hold_seconds)
            response.status_code = status.HTTP_504_GATEWAY_TIMEOUT
            return {"status": "receipt_lost"}
        if not created:
            response.status_code = status.HTTP_200_OK
            return {"status": "duplicate", "notification_no": record["notification_no"]}
        return {"status": "accepted", "notification_no": record["notification_no"]}

    @app.get("/notifications/{notification_no}")
    async def lookup(notification_no: str) -> dict[str, Any]:
        fake.lookups.append(notification_no)
        if fake.lookup_down:
            raise HTTPException(status_code=503, detail="lookup unavailable")
        record = fake.records.get(notification_no)
        if record is None:
            raise HTTPException(status_code=404, detail="unknown notification")
        return record

    @app.get("/notifications")
    def listing() -> dict[str, Any]:
        return {"items": list(fake.records.values())}

    return app
