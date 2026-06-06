"""Phase 1 — organizer deletes a NON-recurring event on main -> propagate to source.

When the user deletes our managed copy of a non-recurring CLIENT event they can
edit on main, and ``DELETE_PROPAGATION_MODE`` is ``on``, BusyBridge deletes the
real source event too — mirroring how an RSVP/edit on main already writes back.

Safety boundaries locked here:
- ``off`` (the default): never propagates — the source survives (only mirrors go).
- ``shadow``: logs only, never arms ``source_delete_pending``.
- recurring series: NEVER propagated by Phase 1 (that is the disarmed _R path).
- non-editable events: never propagated (you can only delete what you organize).

See DELETE_PROPAGATION_PLAN.md (Phase 1).
"""

from __future__ import annotations

import types

import pytest

import app.ledger.ingest.main as main_ingest
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


def _set_mode(monkeypatch, mode: str) -> None:
    stub = types.SimpleNamespace(delete_propagation_mode=mode)
    monkeypatch.setattr(main_ingest, "get_settings", lambda: stub)


def _dates(s: Scenario, nick: str) -> set[str]:
    out: set[str] = set()
    for ev in s.list_events(nick, single_events=True):
        if ev.get("status") == "cancelled":
            continue
        start = ev.get("start", {})
        stamp = start.get("dateTime") or start.get("date") or ""
        if stamp:
            out.add(stamp[:10])
    return out


def _managed_main_id(s: Scenario) -> str:
    """The top-level managed copy on main (the series master for a recurring
    event, or the single event for a non-recurring one)."""
    for ev in s.list_events("main", single_events=False):
        if ev.get("status") != "cancelled":
            return ev["id"]
    raise AssertionError("no managed copy on main")


async def _q(s: Scenario) -> None:
    await s.run_reconciler_until_quiescent("alice", max_passes=6)


async def _given_mirrored_nonrecurring(s: Scenario):
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    user = await s.given_user(
        "alice", main="main", clients=["client_a", "client_b"],
    )
    s.given_event("client_a", summary="One-off", start="2026-04-06T09:00:00Z")
    await _q(s)
    assert "2026-04-06" in _dates(s, "main"), "setup: full copy mirrored to main"
    assert "2026-04-06" in _dates(s, "client_b"), "setup: busy block on peer"
    return user


async def test_off_does_not_propagate_delete(monkeypatch):
    _set_mode(monkeypatch, "off")
    s = Scenario()
    user = await _given_mirrored_nonrecurring(s)
    s.cancel_event("main", _managed_main_id(s))
    await _q(s)
    assert "2026-04-06" in _dates(s, "client_a"), "off: the source must survive"
    assert "2026-04-06" not in _dates(s, "client_b"), "the peer mirror is still removed"
    db = await s.setup_db()
    rows = await (await db.execute(
        "SELECT source_delete_pending FROM ledger_events WHERE user_id = ?",
        (user.user_id,),
    )).fetchall()
    assert all(not r["source_delete_pending"] for r in rows)
    await s.close()


async def test_on_propagates_delete_to_source(monkeypatch):
    _set_mode(monkeypatch, "on")
    s = Scenario()
    await _given_mirrored_nonrecurring(s)
    s.cancel_event("main", _managed_main_id(s))
    await _q(s)
    assert "2026-04-06" not in _dates(s, "client_a"), (
        "on: the real source event must be deleted"
    )
    assert "2026-04-06" not in _dates(s, "client_b"), "the peer mirror is removed"
    await s.close()


async def test_shadow_does_not_delete_or_arm(monkeypatch):
    _set_mode(monkeypatch, "shadow")
    s = Scenario()
    user = await _given_mirrored_nonrecurring(s)
    s.cancel_event("main", _managed_main_id(s))
    await _q(s)
    assert "2026-04-06" in _dates(s, "client_a"), "shadow: the source must survive"
    db = await s.setup_db()
    rows = await (await db.execute(
        "SELECT source_delete_pending FROM ledger_events WHERE user_id = ?",
        (user.user_id,),
    )).fetchall()
    assert all(not r["source_delete_pending"] for r in rows), (
        "shadow mode must never arm a destructive delete"
    )
    await s.close()


async def test_on_does_not_propagate_recurring_series(monkeypatch):
    """Phase 1 must NOT touch recurring events even with the flag on — the
    per-occurrence / _R path stays disarmed (it caused the data loss)."""
    _set_mode(monkeypatch, "on")
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    await s.given_user("alice", main="main", clients=["client_a", "client_b"])
    s.given_recurring_event(
        "client_a", summary="Series", start="2026-04-06T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=4",
    )
    await _q(s)
    # Delete the whole managed series on main.
    s.cancel_event("main", _managed_main_id(s))
    await _q(s)
    # The source series occurrences survive — Phase 1 never propagates recurring.
    assert "2026-04-06" in _dates(s, "client_a"), (
        "a recurring source series must survive Phase-1 propagation"
    )
    await s.close()


async def test_on_does_not_propagate_non_editable_event(monkeypatch):
    """You can only propagate-delete what you organize: an event organized by
    someone else (no edit rights) must never be source-deleted."""
    _set_mode(monkeypatch, "on")
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    user = await s.given_user(
        "alice", main="main", clients=["client_a", "client_b"],
    )
    # Organized by a stranger, alice is a mere attendee, no guestsCanModify.
    s.google.insert_event(s.cal("client_a"), {
        "summary": "Boss's meeting",
        "start": {"dateTime": "2026-04-06T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-04-06T09:30:00Z", "timeZone": "UTC"},
        "organizer": {"email": "boss@external.test"},
        "attendees": [
            {"email": "boss@external.test", "organizer": True,
             "responseStatus": "accepted"},
            {"email": "alice@example.com", "responseStatus": "accepted"},
        ],
    })
    await _q(s)
    db = await s.setup_db()
    led = await (await db.execute(
        """SELECT user_can_edit FROM ledger_events
            WHERE user_id = ? AND source_type = 'client'""",
        (user.user_id,),
    )).fetchone()
    assert led is not None and not led["user_can_edit"], (
        "precondition: this event must be ingested as non-editable"
    )

    s.cancel_event("main", _managed_main_id(s))
    await _q(s)
    assert "2026-04-06" in _dates(s, "client_a"), (
        "a non-editable source event must never be propagation-deleted"
    )
    await s.close()
