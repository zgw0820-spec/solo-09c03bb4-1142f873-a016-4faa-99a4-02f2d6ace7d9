"""Local notification gateway used to exercise alarm escalation end to end.

It behaves like a real paging gateway:

* ``POST /notifications`` accepts a notification idempotently on
  ``notification_no``. Re-posting the same number never pages twice.
* Per-contact rate limiting: at most ``rate_limit_max`` new notifications per
  contact inside any sliding ``rate_limit_window_ms`` window. Excess requests
  get HTTP 429 with a retry-after hint; they are *not* accepted and leave no
  record. Retrying an already-accepted notification number is free: it hits
  the dedupe path and never consumes rate-limit budget. The limiting state is
  rebuilt from the persisted acceptance records, so a restart keeps limiting
  by the original acceptance times.
* ``GET /notifications/{no}`` reports the authoritative receipt, which lets the
  API resolve a lost acknowledgment without resending.
* Failure modes are configurable at runtime, via env or ``PUT /config``:

  - ``slow_ms``: delay before the notification is accepted. If the caller's
    timeout is shorter, the caller sees an indeterminate outcome.
  - ``drop_receipt``: accept and persist, then never answer (or answer with an
    incomplete reply), so the caller's receipt is lost while the gateway still
    knows the truth.
  - ``reject``: refuse the notification outright.
  - ``rate_limit_window_ms`` / ``rate_limit_max``: per-contact acceptance
    budget (0 disables limiting).

Deliveries are recorded so tests can assert exactly-once behaviour.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel, Field


class NotificationIn(BaseModel):
    notification_no: str = Field(min_length=1)
    alarm_no: str
    role: str
    contact: Any
    content_hash: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class GatewayConfig(BaseModel):
    slow_ms: int = Field(default=0, ge=0, le=60_000)
    drop_receipt: bool = False
    reject: bool = False
    # How long a dropped receipt waits before the socket is torn down. Long
    # enough that a client with a short timeout gives up first.
    drop_hold_ms: int = Field(default=3_000, ge=0, le=60_000)
    # Per-contact acceptance budget: at most ``rate_limit_max`` *new*
    # notifications per contact inside any sliding window of
    # ``rate_limit_window_ms`` milliseconds. Excess requests are refused with
    # 429 and a retry-after hint. 0 disables limiting.
    rate_limit_window_ms: int = Field(default=3_000, ge=0, le=60_000)
    rate_limit_max: int = Field(default=1, ge=0, le=100)


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_ts(value: Any) -> float | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def contact_key(contact: Any) -> str:
    """Stable identity of a contact for rate limiting.

    The same person reached on the same channel shares one budget: keyed by
    channel + address when an address is present, otherwise channel + name.
    Accepts the structured form and the JSON-string form so records persisted
    by older builds still map to the right bucket after a restart.
    """
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


class GatewayState:
    def __init__(self, path: str | None = None):
        self.path = path
        self.records: dict[str, dict[str, Any]] = {}
        # contact_key -> acceptance timestamps (epoch seconds). Rebuilt from
        # the persisted records on startup, so a restart keeps enforcing the
        # limit against the original acceptance times.
        self.accept_times: dict[str, list[float]] = {}
        self.config = GatewayConfig(
            slow_ms=int(os.getenv("GATEWAY_SLOW_MS", "0")),
            drop_receipt=os.getenv("GATEWAY_DROP_RECEIPT", "0").lower() in {"1", "true", "yes"},
            reject=os.getenv("GATEWAY_REJECT", "0").lower() in {"1", "true", "yes"},
            rate_limit_window_ms=int(os.getenv("RATE_LIMIT_WINDOW_MS", "3000")),
            rate_limit_max=int(os.getenv("RATE_LIMIT_MAX", "1")),
        )
        self._load()

    def _load(self) -> None:
        if not self.path or not Path(self.path).exists():
            return
        try:
            data = json.loads(Path(self.path).read_text("utf-8"))
            self.records = data.get("records", {})
        except (OSError, ValueError):
            self.records = {}
        self._rebuild_accept_times()

    def _rebuild_accept_times(self) -> None:
        self.accept_times = {}
        for record in self.records.values():
            accepted = _parse_ts(record.get("accepted_at"))
            if accepted is None:
                continue
            key = contact_key(record.get("contact"))
            self.accept_times.setdefault(key, []).append(accepted)

    def _save(self) -> None:
        if not self.path:
            return
        target = Path(self.path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        tmp.write_text(json.dumps({"records": self.records}, ensure_ascii=False, indent=2), "utf-8")
        os.replace(tmp, target)

    def rate_limit_retry_after(self, key: str, window_seconds: float, max_per_window: int) -> float:
        """Seconds until this contact may be accepted again; 0 when allowed."""
        now = time.time()
        recent = [t for t in self.accept_times.get(key, []) if now - t < window_seconds]
        self.accept_times[key] = recent
        if len(recent) >= max_per_window:
            return max(0.0, window_seconds - (now - min(recent)))
        return 0.0

    def note_acceptance(self, key: str, at: float | None = None) -> None:
        self.accept_times.setdefault(key, []).append(at if at is not None else time.time())

    def deliver(self, payload: NotificationIn) -> tuple[bool, dict[str, Any]]:
        """Return (created, record). Idempotent on notification number."""
        existing = self.records.get(payload.notification_no)
        if existing is not None:
            existing["duplicate_requests"] += 1
            self._save()
            return False, existing
        record = {
            "notification_no": payload.notification_no,
            "alarm_no": payload.alarm_no,
            "role": payload.role,
            "contact": payload.contact,
            "content_hash": payload.content_hash,
            "payload": payload.payload,
            "status": "accepted",
            "deliveries": 1,
            "duplicate_requests": 0,
            "accepted_at": _now(),
        }
        self.records[payload.notification_no] = record
        self._save()
        return True, record


_STATE: GatewayState | None = None


def state() -> GatewayState:
    global _STATE
    if _STATE is None:
        _STATE = GatewayState(os.getenv("GATEWAY_STATE_PATH"))
    return _STATE


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _STATE
    _STATE = GatewayState(os.getenv("GATEWAY_STATE_PATH"))
    yield


app = FastAPI(
    title="本地通知网关（测试用）",
    version="1.1.0",
    description=(
        "可按通知号去重、可设置慢响应/丢回执的本地通知网关。\n\n"
        "**按联系人限流**：同一联系人在任意连续 `rate_limit_window_ms` 内最多受理 "
        "`rate_limit_max` 条新通知，超额返回 429 与可重试时间；已受理通知号的重试"
        "直接去重，不占限流额度。重启后按持久化的受理记录继续限流。"
    ),
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "recorded": len(state().records), "config": state().config.model_dump()}


@app.get("/config", response_model=GatewayConfig)
def get_config() -> GatewayConfig:
    return state().config


@app.put("/config", response_model=GatewayConfig)
def set_config(config: GatewayConfig) -> GatewayConfig:
    state().config = config
    return config


@app.delete("/admin/reset")
def reset() -> dict[str, Any]:
    """Clear all records and restore defaults. Test convenience."""
    s = state()
    s.records.clear()
    s.accept_times.clear()
    s.config = GatewayConfig()
    s._save()
    return {"status": "reset"}


@app.post(
    "/notifications",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        200: {"description": "已受理过的通知号重试（幂等去重，不占限流额度）"},
        429: {"description": "同一联系人超出限流额度；通知未受理，按 retry_after_seconds 重试"},
    },
)
async def accept_notification(payload: NotificationIn, response: Response) -> dict[str, Any]:
    s = state()
    config = s.config

    if config.reject:
        raise HTTPException(status_code=422, detail="gateway configured to reject notifications")

    # Rate limiting applies to *new* notification numbers only. A retry of an
    # already-accepted number falls through to the dedupe path below and never
    # consumes budget.
    if (
        payload.notification_no not in s.records
        and config.rate_limit_window_ms > 0
        and config.rate_limit_max > 0
    ):
        key = contact_key(payload.contact)
        wait = s.rate_limit_retry_after(
            key, config.rate_limit_window_ms / 1000, config.rate_limit_max
        )
        if wait > 0:
            response.status_code = status.HTTP_429_TOO_MANY_REQUESTS
            response.headers["Retry-After"] = str(math.ceil(wait))
            return {
                "status": "rate_limited",
                "notification_no": payload.notification_no,
                "detail": (
                    f"contact is limited to {config.rate_limit_max} notification(s) "
                    f"per {config.rate_limit_window_ms}ms"
                ),
                "retry_after_seconds": round(wait, 3),
            }
        # Reserve the budget before any artificial delay, so concurrent posts
        # for the same contact are limited correctly even while one is slow.
        s.note_acceptance(key)

    if config.slow_ms:
        # The delay happens *before* acceptance: a caller that times out cannot
        # know whether acceptance happened, which is exactly the ambiguous case
        # the API must handle without false escalation.
        await asyncio.sleep(config.slow_ms / 1000)

    created, record = s.deliver(payload)

    if config.drop_receipt:
        # The notification is now accepted and durable, but the caller never
        # learns that. Hold the connection past any reasonable client timeout.
        await asyncio.sleep(config.drop_hold_ms / 1000)
        response.status_code = status.HTTP_504_GATEWAY_TIMEOUT
        return {"status": "receipt_lost"}

    if not created:
        response.status_code = status.HTTP_200_OK
        return {
            "status": "duplicate",
            "notification_no": record["notification_no"],
            "accepted_at": record["accepted_at"],
            "deliveries": record["deliveries"],
        }
    return {
        "status": "accepted",
        "notification_no": record["notification_no"],
        "accepted_at": record["accepted_at"],
        "deliveries": record["deliveries"],
    }


@app.get("/notifications/{notification_no}")
async def lookup(notification_no: str) -> dict[str, Any]:
    record = state().records.get(notification_no)
    if record is None:
        raise HTTPException(status_code=404, detail="unknown notification")
    return record


@app.get("/notifications")
def list_notifications(role: str | None = None, alarm_no: str | None = None) -> dict[str, Any]:
    records = list(state().records.values())
    if role:
        records = [r for r in records if r["role"] == role]
    if alarm_no:
        records = [r for r in records if r["alarm_no"] == alarm_no]
    return {"items": records, "total": len(records)}
