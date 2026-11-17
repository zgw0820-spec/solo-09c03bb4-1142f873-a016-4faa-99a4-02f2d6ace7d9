"""Contact-based rate limiting (gateway 429) and API redelivery.

Covers the machine-room scenario: several cabinets overheat at once and the
same on-duty contact must still receive every notification, paced by the
gateway's capacity (one acceptance per contact per sliding window).
"""

from __future__ import annotations

import asyncio

import httpx

from tests.conftest import ApiHarness
from tests.helpers import _get, _status


def _primary(view: dict) -> dict:
    return next(n for n in view["notifications"] if n["role"] == "primary")


def _backup(view: dict) -> dict | None:
    return next((n for n in view["notifications"] if n["role"] == "backup"), None)


async def _primary_gateway_status(api: ApiHarness, alarm_no: str, status: str) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    if response.status_code != 200:
        return False
    view = response.json()
    primaries = [n for n in view["notifications"] if n["role"] == "primary"]
    return bool(primaries) and primaries[0]["gateway_status"] == status


async def _backup_count(api: ApiHarness, expected: int) -> bool:
    return len(api.fake.role_notifications("backup")) == expected


async def _primary_count(api: ApiHarness, expected: int) -> bool:
    return len(api.fake.role_notifications("primary")) == expected


async def _throttled_backup_alarm(api: ApiHarness, alarm_nos: list[str]) -> str | None:
    """Return the alarm whose backup is parked by the rate limiter, if any."""
    for alarm_no in alarm_nos:
        response = await api.client.get(f"/alarms/{alarm_no}")
        if response.status_code != 200:
            continue
        backup = _backup(response.json())
        if (
            backup
            and backup["state"] == "pending"
            and backup["gateway_status"] == "rate_limited"
        ):
            return alarm_no
    return None


async def test_gateway_429_carries_retry_after_and_duplicate_is_free(api: ApiHarness):
    """Gateway contract: the second new notification for a contact inside the
    window gets 429 + a retry-after hint and leaves no record; retrying an
    already-accepted number is a free duplicate; after the window the throttled
    notification is accepted."""
    api.fake.rate_limit_window_seconds = 0.8
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.gateway_app), base_url="http://gw.test"
    )
    try:
        contact = {"name": "张工", "channel": "app"}
        first = {
            "notification_no": "N-RL-1",
            "alarm_no": "G-1",
            "role": "primary",
            "contact": contact,
            "content_hash": "h",
            "payload": {},
        }
        second = {**first, "notification_no": "N-RL-2"}

        r1 = await client.post("/notifications", json=first)
        assert r1.status_code == 202, r1.text

        r2 = await client.post("/notifications", json=second)
        assert r2.status_code == 429, r2.text
        body = r2.json()
        assert body["status"] == "rate_limited"
        assert 0 < body["retry_after_seconds"] <= 0.8
        assert "retry-after" in r2.headers
        # A 429 is not an acceptance: no record, no delivery.
        assert "N-RL-2" not in api.fake.records

        # Retrying the already-accepted number does not consume budget.
        r3 = await client.post("/notifications", json=first)
        assert r3.status_code == 200 and r3.json()["status"] == "duplicate"

        # Once the window slides past, the throttled notification is accepted.
        await asyncio.sleep(0.85)
        r4 = await client.post("/notifications", json=second)
        assert r4.status_code == 202, r4.text
    finally:
        await client.aclose()


async def test_throttled_primary_is_pending_then_redelivered(api: ApiHarness):
    """Two alarms for the same contact: the second primary is 429'd. The API
    must treat it as pending (not rejected, not unknown), must not start the
    confirmation clock, and must redeliver after the retry-after delay. The
    first alarm is unaffected."""
    api.fake.rate_limit_window_seconds = 0.6
    await api.client.post("/alarms", json=api.alarm_payload("RL-A", deadline=5))
    assert await api.wait_until(lambda: _status(api, "RL-A", "awaiting_confirmation"))

    await api.client.post("/alarms", json=api.alarm_payload("RL-B", deadline=5))
    throttled = await api.wait_until(
        lambda: _primary_gateway_status(api, "RL-B", "rate_limited"), timeout=5
    )
    assert throttled, await _get(api, "RL-B")
    view = await _get(api, "RL-B")
    # Rate-limited means "still to be delivered": no acceptance, no clock.
    assert view["status"] == "primary_queued"
    assert view["primary_accepted_at"] is None
    assert view["confirmation_due_at"] is None
    primary = _primary(view)
    assert primary["state"] == "pending"
    assert primary["display_status"] == "rate_limited"
    assert any("限流" in w for w in view["warnings"])

    # After the window the notification is redelivered and the clock starts
    # from the real acceptance, not from submission.
    assert await api.wait_until(lambda: _status(api, "RL-B", "awaiting_confirmation"), timeout=6)
    view = await _get(api, "RL-B")
    assert view["primary_accepted_at"] is not None
    assert view["confirmation_due_at"] is not None

    primaries = api.fake.role_notifications("primary")
    assert len(primaries) == 2
    assert all(r["deliveries"] == 1 for r in primaries)
    by_alarm = {r["alarm_no"]: r for r in primaries}
    assert by_alarm["RL-B"]["accepted_at"] > by_alarm["RL-A"]["accepted_at"]


async def test_other_contacts_are_not_delayed_by_throttling(api: ApiHarness):
    """While one contact is being throttled, other contacts deliver as usual."""
    api.fake.rate_limit_window_seconds = 2.0
    await api.client.post(
        "/alarms",
        json=api.alarm_payload(
            "OC-1", deadline=5, primary_contact={"name": "限流联系人", "channel": "app"}
        ),
    )
    assert await api.wait_until(lambda: _status(api, "OC-1", "awaiting_confirmation"))

    # Same contact (will be throttled for ~2s) and a different contact.
    await api.client.post(
        "/alarms",
        json=api.alarm_payload(
            "OC-2", deadline=5, primary_contact={"name": "限流联系人", "channel": "app"}
        ),
    )
    await api.client.post(
        "/alarms",
        json=api.alarm_payload(
            "OC-3", deadline=5, primary_contact={"name": "其他联系人", "channel": "app"}
        ),
    )

    # The unrelated contact is accepted well inside the throttled contact's
    # window — proof the limiter does not serialize unrelated deliveries.
    assert await api.wait_until(
        lambda: _status(api, "OC-3", "awaiting_confirmation"), timeout=1.5
    ), await _get(api, "OC-3")
    # The throttled one catches up after its retry-after delay.
    assert await api.wait_until(
        lambda: _status(api, "OC-2", "awaiting_confirmation"), timeout=6
    ), await _get(api, "OC-2")
    assert len(api.fake.role_notifications("primary")) == 3


async def test_backup_throttled_then_redelivered(api: ApiHarness):
    """Two alarms escalate at the same time and share one backup contact: the
    second backup notification is 429'd and must still be delivered after the
    retry-after delay."""
    api.fake.rate_limit_window_seconds = 0.5
    await api.client.post(
        "/alarms",
        json=api.alarm_payload(
            "BRL-1", deadline=2, primary_contact={"name": "主一", "channel": "app"}
        ),
    )
    await api.client.post(
        "/alarms",
        json=api.alarm_payload(
            "BRL-2", deadline=2, primary_contact={"name": "主二", "channel": "app"}
        ),
    )

    assert await api.wait_until(lambda: _status(api, "BRL-1", "escalated"), timeout=8)
    assert await api.wait_until(lambda: _status(api, "BRL-2", "escalated"), timeout=8)

    # Both backup notifications are eventually accepted, exactly once each.
    ok = await api.wait_until(lambda: _backup_count(api, 2), timeout=8)
    assert ok, "throttled backup was never redelivered"
    backups = api.fake.role_notifications("backup")
    assert all(r["deliveries"] == 1 for r in backups)
    for alarm_no in ("BRL-1", "BRL-2"):
        view = await _get(api, alarm_no)
        assert _backup(view)["state"] == "accepted"


async def test_backup_throttled_then_cancelled_by_confirmation(api: ApiHarness):
    """A backup notification that is waiting out the rate limit must be
    cancelled when the alarm is confirmed before the actual send."""
    api.fake.rate_limit_window_seconds = 1.2
    await api.client.post(
        "/alarms",
        json=api.alarm_payload(
            "BRC-1", deadline=2, primary_contact={"name": "主甲", "channel": "app"}
        ),
    )
    await api.client.post(
        "/alarms",
        json=api.alarm_payload(
            "BRC-2", deadline=2, primary_contact={"name": "主乙", "channel": "app"}
        ),
    )
    assert await api.wait_until(lambda: _status(api, "BRC-1", "escalated"), timeout=8)
    assert await api.wait_until(lambda: _status(api, "BRC-2", "escalated"), timeout=8)

    # One of the two backups got 429'd; find which alarm it belongs to.
    alarm_no: str | None = None

    async def probe() -> bool:
        nonlocal alarm_no
        alarm_no = await _throttled_backup_alarm(api, ["BRC-1", "BRC-2"])
        return alarm_no is not None

    ok = await api.wait_until(probe, timeout=5)
    assert ok, "expected one backup to be rate-limited"
    assert alarm_no is not None

    confirm = await api.client.post(
        f"/alarms/{alarm_no}/confirm", json={"confirmed_by": "张工"}
    )
    assert confirm.status_code == 200

    # Wait past the retry-after moment: the cancelled backup must not be sent.
    await asyncio.sleep(1.6)
    view = await _get(api, alarm_no)
    backup = _backup(view)
    assert backup["state"] == "cancelled", backup
    assert all(
        r["alarm_no"] != alarm_no for r in api.fake.role_notifications("backup")
    ), "cancelled backup was delivered after confirmation"


async def test_concurrent_same_contact_alarms_all_delivered_once(api: ApiHarness):
    """A burst of simultaneous alarms for one contact: every notification is
    delivered exactly once, paced by the gateway window."""
    api.fake.rate_limit_window_seconds = 0.3

    async def submit(index: int):
        return await api.client.post(
            "/alarms", json=api.alarm_payload(f"CC-{index}", deadline=6)
        )

    results = await asyncio.gather(*(submit(i) for i in range(5)))
    assert all(r.status_code == 201 for r in results)

    ok = await api.wait_until(lambda: _primary_count(api, 5), timeout=15)
    assert ok, f"only {len(api.fake.role_notifications('primary'))}/5 delivered"
    records = api.fake.role_notifications("primary")
    assert len({r["notification_no"] for r in records}) == 5
    assert all(r["deliveries"] == 1 for r in records)


async def test_throttled_delivery_resumes_after_api_restart(tmp_path):
    """The API restarts while a notification waits out the rate limit: the
    pending redelivery is rebuilt from the database and completes exactly once."""
    from tests.fake_gateway import FakeGateway
    from tests.test_recovery import RestartableApi, payload

    fake = FakeGateway()
    fake.rate_limit_window_seconds = 0.6
    db_path = str(tmp_path / "alarms.db")
    first = await RestartableApi(db_path, fake, "http://gw.test").start()
    await first.client.post("/alarms", json=payload("RS-1", deadline=10))
    assert await first.wait_until(lambda: _accepted(first, "RS-1"), timeout=8)
    await first.client.post("/alarms", json=payload("RS-2", deadline=10))
    throttled = await first.wait_until(
        lambda: _primary_gateway_status(first, "RS-2", "rate_limited"), timeout=6
    )
    assert throttled, (await first.client.get("/alarms/RS-2")).text
    await first.stop()

    second = await RestartableApi(db_path, fake, "http://gw.test").start()
    try:
        assert await second.wait_until(lambda: _accepted(second, "RS-2"), timeout=8), (
            await second.client.get("/alarms/RS-2")
        ).text
        primaries = fake.role_notifications("primary")
        assert len(primaries) == 2, "restart lost or duplicated a throttled notification"
        assert all(r["deliveries"] == 1 for r in primaries)
    finally:
        await second.stop()


async def _accepted(api, alarm_no: str) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    if response.status_code != 200:
        return False
    return response.json()["primary_accepted_at"] is not None


def test_gateway_rate_limit_state_survives_restart(tmp_path):
    """The gateway rebuilds its per-contact limiter from the persisted
    acceptance records, so a restart keeps limiting by the original times."""
    from gateway.main import GatewayState, NotificationIn, contact_key

    path = str(tmp_path / "gateway.json")
    contact = {"name": "张工", "channel": "app"}
    first = GatewayState(path)
    created, _record = first.deliver(
        NotificationIn(
            notification_no="N-1", alarm_no="A-1", role="primary", contact=contact
        )
    )
    assert created

    # Simulate a process restart: fresh state object over the same file.
    second = GatewayState(path)
    wait = second.rate_limit_retry_after(contact_key(contact), 3.0, 1)
    assert wait > 0, "rate limit was lost across a gateway restart"
    # A different contact is unaffected.
    assert second.rate_limit_retry_after("app:别人", 3.0, 1) == 0.0
