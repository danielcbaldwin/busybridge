"""Origin writebacks send Google's standard notifications.

Product decision: the ``events.patch`` the outbox fires at the user's
REAL source event (the ``present_full_rsvp_only`` origin-writeback
projection) carries ``sendUpdates='all'`` when the
``writeback_notifications`` setting is on (the default) — so the
organizer receives the standard accepted/declined email exactly as if
the user had responded on the client calendar directly.  With the
setting off, the patch keeps the historical silent behaviour.

Every OTHER write — creates/updates/deletes of our own managed copies —
stays silent unconditionally regardless of the setting: those events
have no real attendees and convergence churn must never spray email.

The fake records ``(operation, event_id, send_updates)`` per write in
``FakeGoogleCalendar.send_updates_log``; the flow tests assert against
that.  The setting is read in ``app.ledger.outbox._do_patch`` at
execution time via the module-level ``get_settings`` reference —
``get_settings()`` is lru_cached, so tests override by monkeypatching
``app.ledger.outbox.get_settings`` (the same idiom as the planner
settings tests) rather than mutating the cached instance.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


def _client_event_with_attendees(s: Scenario, calendar_nick: str, event_id: str):
    """Insert a client event where alice is organizer + a self
    attendee, plus a second guest (bob) — same shape as the RSVP
    writeback scenario in test_rsvp_writeback.py."""
    return s.google.insert_event(s.cal(calendar_nick), {
        "id": event_id,
        "summary": "Team sync",
        "start": {"dateTime": "2026-02-02T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-02-02T09:30:00Z", "timeZone": "UTC"},
        "organizer": {"email": "alice@example.com"},
        "attendees": [
            {"email": "alice@example.com", "self": True,
             "responseStatus": "needsAction"},
            {"email": "bob@example.com", "responseStatus": "accepted"},
        ],
    })


def _attendee(event: dict, email: str):
    for a in event.get("attendees") or []:
        if a.get("email") == email:
            return a
    return None


async def _rsvp_writeback_run(s: Scenario, source_event_id: str) -> None:
    """Set up the standard RSVP-writeback flow, clear the write log,
    then run the reconcile pass that delivers the origin patch (plus
    the accompanying managed-copy writes)."""
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])
    _client_event_with_attendees(s, "client_a", source_event_id)
    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    # Alice accepts on the main copy.
    main_copy = s.assert_event_exists("main", summary="Team sync")
    s.update_event(
        "main", main_copy["id"],
        attendees=[{"email": "alice@example.com", "self": True,
                    "responseStatus": "accepted"}],
    )
    # Only the writeback run is under test — drop the setup writes.
    s.google.send_updates_log.clear()
    await s.run_reconciler_until_quiescent("alice", max_passes=5)


async def test_origin_patch_sends_standard_notifications_when_setting_on(monkeypatch):
    monkeypatch.setattr(
        "app.ledger.outbox.get_settings",
        lambda: SimpleNamespace(writeback_notifications=True),
    )
    s = Scenario()
    try:
        await _rsvp_writeback_run(s, "notifon000001")

        # The RSVP reached the origin source event...
        origin = s.google.get_event(s.cal("client_a"), "notifon000001")
        alice = _attendee(origin, "alice@example.com")
        assert alice is not None and alice["responseStatus"] == "accepted"

        log = s.google.send_updates_log
        # ...the origin patch carried sendUpdates='all'...
        origin_patches = [
            e for e in log if e[0] == "patch" and e[1] == "notifon000001"
        ]
        assert origin_patches, f"no origin patch recorded; log={log}"
        assert all(v == "all" for (_, _, v) in origin_patches), (
            f"origin patch was not sendUpdates='all'; log={log}"
        )
        # ...it was the ONLY write in the run that notified...
        notified = [e for e in log if e[2] is not None]
        assert notified == origin_patches, (
            f"a non-origin write asked for notifications; log={log}"
        )
        # ...and the same run really did contain managed-copy writes,
        # all of them silent (they cannot even carry send_updates).
        managed = [e for e in log if e[1] != "notifon000001"]
        assert managed, f"expected managed-copy writes in the run; log={log}"
        assert all(v is None for (_, _, v) in managed)
    finally:
        await s.close()


async def test_origin_patch_is_silent_when_setting_off(monkeypatch):
    monkeypatch.setattr(
        "app.ledger.outbox.get_settings",
        lambda: SimpleNamespace(writeback_notifications=False),
    )
    s = Scenario()
    try:
        await _rsvp_writeback_run(s, "notifoff00001")

        # The RSVP still reaches the origin (the writeback itself is
        # not gated by the setting — only the notification is)...
        origin = s.google.get_event(s.cal("client_a"), "notifoff00001")
        alice = _attendee(origin, "alice@example.com")
        assert alice is not None and alice["responseStatus"] == "accepted"

        log = s.google.send_updates_log
        origin_patches = [
            e for e in log if e[0] == "patch" and e[1] == "notifoff00001"
        ]
        assert origin_patches, f"no origin patch recorded; log={log}"
        # ...but silently: nothing in the entire run notified.
        assert all(v is None for (_, _, v) in log), (
            f"a write asked for notifications with the setting off; log={log}"
        )
    finally:
        await s.close()


# ---------------------------------------------------------------------------
# RealGoogleClient: sendUpdates / legacy sendNotifications exclusivity
# ---------------------------------------------------------------------------
class _FakeRequest:
    """Stands in for a googleapiclient HttpRequest (mirrors the helper
    in test_real_google_client.py)."""

    def __init__(self, result):
        self.headers: dict = {}
        self._result = result

    def execute(self):
        return self._result


def _real_client(monkeypatch):
    from app.ledger import real_google_client as rgc

    service = MagicMock()
    service.events.return_value.patch.return_value = _FakeRequest({"id": "e1"})
    monkeypatch.setattr("app.auth.google.build", lambda *a, **k: service)
    return rgc.RealGoogleClient(MagicMock()), service


async def test_real_patch_passes_send_updates_and_never_the_legacy_param(monkeypatch):
    """With send_updates set, the request carries ONLY the current
    ``sendUpdates`` API parameter — never both it and the deprecated
    ``sendNotifications`` on the same call."""
    client, service = _real_client(monkeypatch)
    client.patch_event("cal", "e1", {"attendees": []}, send_updates="all")
    kwargs = service.events.return_value.patch.call_args.kwargs
    assert kwargs.get("sendUpdates") == "all"
    assert "sendNotifications" not in kwargs


async def test_real_patch_defaults_to_the_historical_silent_request(monkeypatch):
    """Without send_updates the request shape is unchanged from before:
    legacy ``sendNotifications=False``, no ``sendUpdates``."""
    client, service = _real_client(monkeypatch)
    client.patch_event("cal", "e1", {"attendees": []})
    kwargs = service.events.return_value.patch.call_args.kwargs
    assert kwargs.get("sendNotifications") is False
    assert "sendUpdates" not in kwargs


async def test_fake_patch_records_send_updates_per_call():
    """The fake logs (operation, event_id, send_updates) per write and
    defaults to silent when the argument is omitted."""
    from tests.fakes.google_calendar import FakeGoogleCalendar

    fake = FakeGoogleCalendar()
    fake.add_calendar("cal")
    fake.insert_event("cal", {"id": "logdemo000001", "summary": "x"})
    fake.patch_event("cal", "logdemo000001", {"summary": "y"})
    fake.patch_event("cal", "logdemo000001", {"summary": "z"}, send_updates="all")
    assert fake.send_updates_log == [
        ("insert", "logdemo000001", None),
        ("patch", "logdemo000001", None),
        ("patch", "logdemo000001", "all"),
    ]
