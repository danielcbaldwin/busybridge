"""End-to-end smoke tests for the integration framework.

These tests exercise the framework against the fake Google to
verify the foundation works before the Stage-2 ledger code lands.
They will become the lower layer of "given X, run Y, assert Z"
scenarios once the new system can be plugged in.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.fakes.failures import NetworkError
from tests.fakes.google_calendar import GoogleApiError
from tests.integration.framework import Scenario, ScenarioAssertionError

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Construction & calendar registration
# ---------------------------------------------------------------------------
def test_scenario_starts_clean():
    s = Scenario()
    assert s.clock.now() == datetime(2026, 1, 1, tzinfo=UTC)
    assert s.failures.total_failures == 0
    assert s.google.list_calendars()["items"] == []


def test_given_calendar_registers_and_resolves_nickname():
    s = Scenario()
    cal_id = s.given_calendar("main")
    assert s.cal("main") == cal_id
    assert s.nick(cal_id) == "main"


def test_given_calendar_duplicate_nickname_rejected():
    s = Scenario()
    s.given_calendar("main")
    with pytest.raises(ValueError):
        s.given_calendar("main")


def test_unknown_nickname_helpful_error():
    s = Scenario()
    s.given_calendar("main")
    with pytest.raises(KeyError, match="known: \\['main'\\]"):
        s.cal("client_a")


# ---------------------------------------------------------------------------
# given_event / given_recurring_event
# ---------------------------------------------------------------------------
def test_given_event_creates_event():
    s = Scenario()
    s.given_calendar("main")
    out = s.given_event(
        "main",
        summary="Standup",
        start="2026-02-02T09:00:00Z",
    )
    assert out["summary"] == "Standup"
    s.assert_event_count("main", 1)


def test_given_event_supports_attendees_and_extended_properties():
    s = Scenario()
    s.given_calendar("client_a")
    s.given_event(
        "client_a",
        summary="Meeting",
        start="2026-02-02T09:00:00Z",
        attendees=[{"email": "alice@example.com", "responseStatus": "accepted"}],
        extended_properties={"private": {"bb_origin_id": "src1"}},
    )
    found = s.find_events("client_a", summary="Meeting")
    assert len(found) == 1
    assert found[0]["attendees"][0]["email"] == "alice@example.com"
    assert found[0]["extendedProperties"]["private"]["bb_origin_id"] == "src1"


def test_given_event_with_explicit_id():
    s = Scenario()
    s.given_calendar("main")
    out = s.given_event(
        "main",
        start="2026-02-02T09:00:00Z",
        event_id="bbsmoke00000001",
    )
    assert out["id"] == "bbsmoke00000001"


def test_given_recurring_event_creates_series():
    s = Scenario()
    s.given_calendar("main")
    out = s.given_recurring_event(
        "main",
        summary="Weekly",
        start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=3;BYDAY=MO",
    )
    assert out["recurrence"] == ["RRULE:FREQ=WEEKLY;COUNT=3;BYDAY=MO"]
    insts = s.google.list_instances(s.cal("main"), out["id"])
    assert len(insts["items"]) == 3


def test_given_event_supports_all_day():
    s = Scenario()
    s.given_calendar("main")
    out = s.given_event("main", summary="All day", start="2026-02-02")
    assert out["start"] == {"date": "2026-02-02"}
    assert out["end"] == {"date": "2026-02-03"}


# ---------------------------------------------------------------------------
# advance / clock
# ---------------------------------------------------------------------------
def test_advance_moves_clock():
    s = Scenario()
    before = s.clock.now()
    s.advance(timedelta(hours=1))
    assert s.clock.now() == before + timedelta(hours=1)


def test_advance_returns_callbacks_fired():
    s = Scenario()
    fired: list[int] = []
    s.clock.schedule(60, lambda: fired.append(1))
    n = s.advance(120)
    assert n == 1
    assert fired == [1]


# ---------------------------------------------------------------------------
# update_event helper
# ---------------------------------------------------------------------------
def test_update_event_helper_overlays_fields():
    s = Scenario()
    s.given_calendar("main")
    out = s.given_event("main", summary="Original", start="2026-02-02T09:00:00Z")
    s.update_event("main", out["id"], summary="Renamed")
    refetched = s.google.get_event(s.cal("main"), out["id"])
    assert refetched["summary"] == "Renamed"
    # Start preserved
    assert refetched["start"]["dateTime"] == "2026-02-02T09:00:00Z"


def test_update_event_with_new_start_recomputes_end():
    s = Scenario()
    s.given_calendar("main")
    out = s.given_event("main", summary="X", start="2026-02-02T09:00:00Z")
    s.update_event(
        "main", out["id"],
        start="2026-02-02T11:00:00Z",
        duration_minutes=45,
    )
    refetched = s.google.get_event(s.cal("main"), out["id"])
    assert refetched["start"]["dateTime"] == "2026-02-02T11:00:00Z"
    assert refetched["end"]["dateTime"] == "2026-02-02T11:45:00Z"


# ---------------------------------------------------------------------------
# find_events / assert_event_exists
# ---------------------------------------------------------------------------
def test_find_events_by_summary():
    s = Scenario()
    s.given_calendar("main")
    s.given_event("main", summary="A", start="2026-02-02T09:00:00Z")
    s.given_event("main", summary="B", start="2026-02-03T09:00:00Z")
    s.given_event("main", summary="A", start="2026-02-04T09:00:00Z")
    assert len(s.find_events("main", summary="A")) == 2
    assert len(s.find_events("main", summary="B")) == 1
    assert len(s.find_events("main", summary="missing")) == 0


def test_find_events_by_summary_contains():
    s = Scenario()
    s.given_calendar("main")
    s.given_event("main", summary="🔒 Locked Meeting", start="2026-02-02T09:00:00Z")
    s.given_event("main", summary="Open Meeting", start="2026-02-03T09:00:00Z")
    locked = s.find_events("main", summary_contains="Locked")
    assert len(locked) == 1
    assert locked[0]["summary"] == "🔒 Locked Meeting"


def test_find_events_by_start_iso_string():
    s = Scenario()
    s.given_calendar("main")
    s.given_event("main", summary="A", start="2026-02-02T09:00:00Z")
    s.given_event("main", summary="B", start="2026-02-03T09:00:00Z")
    found = s.find_events("main", start="2026-02-02T09:00:00Z")
    assert [e["summary"] for e in found] == ["A"]


def test_assert_event_exists_exact_match():
    s = Scenario()
    s.given_calendar("main")
    s.given_event("main", summary="Standup", start="2026-02-02T09:00:00Z")
    out = s.assert_event_exists("main", summary="Standup")
    assert out["summary"] == "Standup"


def test_assert_event_exists_raises_when_zero():
    s = Scenario()
    s.given_calendar("main")
    with pytest.raises(ScenarioAssertionError, match="found 0"):
        s.assert_event_exists("main", summary="missing")


def test_assert_event_exists_raises_when_multiple():
    s = Scenario()
    s.given_calendar("main")
    s.given_event("main", summary="dup", start="2026-02-02T09:00:00Z")
    s.given_event("main", summary="dup", start="2026-02-03T09:00:00Z")
    with pytest.raises(ScenarioAssertionError, match="found 2"):
        s.assert_event_exists("main", summary="dup")


def test_assert_no_event_with_summary():
    s = Scenario()
    s.given_calendar("main")
    s.assert_no_event_with_summary("main", "absent")
    s.given_event("main", summary="present", start="2026-02-02T09:00:00Z")
    s.assert_no_event_with_summary("main", "absent")
    with pytest.raises(ScenarioAssertionError):
        s.assert_no_event_with_summary("main", "present")


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------
def test_cancel_event_marks_cancelled():
    s = Scenario()
    s.given_calendar("main")
    out = s.given_event("main", summary="will cancel", start="2026-02-02T09:00:00Z")
    s.cancel_event("main", out["id"])
    assert s.find_events("main", summary="will cancel") == []
    cancelled = s.find_events("main", summary="will cancel", status="cancelled")
    assert len(cancelled) == 1


# ---------------------------------------------------------------------------
# _R reschedule integration
# ---------------------------------------------------------------------------
def test_reschedule_recurring_via_helper():
    s = Scenario()
    s.given_calendar("main")
    parent = s.given_recurring_event(
        "main",
        summary="Weekly",
        start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=10",
        event_id="bbeekla0000001",
    )
    new = s.reschedule_recurring_this_and_following(
        "main",
        parent["id"],
        from_dt="2026-02-16T09:00:00Z",
        new_start="2026-02-16T11:00:00Z",
        new_summary="Moved",
        new_rrule="RRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=8",
    )
    assert new["id"].endswith("_R20260216T090000Z")
    assert new["summary"] == "Moved"


# ---------------------------------------------------------------------------
# Failure injection wiring
# ---------------------------------------------------------------------------
def test_failures_propagate_through_framework():
    s = Scenario()
    s.given_calendar("main")
    s.failures.network_error_rate = 1.0
    with pytest.raises(NetworkError):
        s.given_event("main", summary="X", start="2026-02-02T09:00:00Z")


def test_force_failure_via_failures_attr():
    s = Scenario()
    s.given_calendar("main")
    s.failures.force_next(GoogleApiError(503, "Service Unavailable", "x"))
    with pytest.raises(GoogleApiError) as exc_info:
        s.given_event("main", summary="X", start="2026-02-02T09:00:00Z")
    assert exc_info.value.status == 503


def test_seeded_scenarios_are_deterministic():
    def trace(seed: int) -> list[str]:
        s = Scenario(seed=seed)
        s.given_calendar("main")
        s.failures.network_error_rate = 0.5
        results: list[str] = []
        for i in range(20):
            try:
                s.given_event(
                    "main", summary=f"e{i}",
                    start=f"2026-02-{(i % 28) + 1:02d}T09:00:00Z",
                )
                results.append("ok")
            except NetworkError:
                results.append("net")
        return results

    # Same seed → identical trace.
    assert trace(42) == trace(42)


# ---------------------------------------------------------------------------
# Multi-calendar story (the shape Stage 2/3 tests will take)
# ---------------------------------------------------------------------------
def test_multi_calendar_setup_with_pagination():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    s.given_calendar("client_c")

    # 12 events on each client.
    for cal in ("client_a", "client_b", "client_c"):
        for i in range(12):
            s.given_event(
                cal,
                summary=f"{cal}-event-{i}",
                start=f"2026-02-{i + 1:02d}T09:00:00Z",
            )

    # Each list_events call should auto-paginate via the helper.
    for cal in ("client_a", "client_b", "client_c"):
        events = s.list_events(cal)
        assert len(events) == 12

    # Main is empty until reconciler exists.
    s.assert_no_events("main")


def test_clock_advance_with_sync_token_expiry():
    """Models a realistic story: token issued, time passes, token
    expires, full sync recovers.  This is the kind of scenario the
    rewrite's recurring-cancellation amnesia regression test will
    look like."""
    s = Scenario(sync_token_ttl=timedelta(hours=1))
    s.given_calendar("main")
    s.given_event("main", summary="A", start="2026-02-02T09:00:00Z")
    sync = s.google.list_events(s.cal("main"))["nextSyncToken"]
    s.advance(timedelta(hours=2))
    with pytest.raises(GoogleApiError) as exc_info:
        s.google.list_events(s.cal("main"), sync_token=sync)
    assert exc_info.value.status == 410
    # Full sync still works.
    fresh = s.list_events("main")
    assert len(fresh) == 1
