"""Tests for recurring-event behaviour of the fake Google Calendar.

Covers:

* ``derive_instance_event_id`` helper.
* ``events.get`` with a derived instance ID synthesises an instance
  when no override row exists.
* ``events.update``/``events.delete`` on a derived instance ID
  materialises an exception entry on the parent series.
* ``events.instances`` returns synthesized + overridden instances
  with proper ``show_deleted`` semantics.
* ``events.list(singleEvents=True)`` expands recurring parents.
* ``events.list(singleEvents=False, showDeleted=True)`` reproduces
  the **full-sync-omits-cancelled-instances** quirk: cancelled
  instance overrides are not returned even with ``showDeleted=True``.
* ``reschedule_series_this_and_following`` simulates the ``_R``
  reschedule quirk: original series is truncated, future overrides
  are cancelled, and a new ``<parent>_R<stamp>`` event is created.
* Incremental sync DOES surface cancelled instance overrides.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.fakes.clock import SimulatedClock
from tests.fakes.google_calendar import (
    FakeGoogleCalendar,
    GoogleApiError,
    derive_instance_event_id,
)

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def clock() -> SimulatedClock:
    return SimulatedClock()


@pytest.fixture
def fake(clock: SimulatedClock) -> FakeGoogleCalendar:
    g = FakeGoogleCalendar(clock=clock)
    g.add_calendar("primary")
    return g


def _weekly_body(
    *,
    start: str = "2026-02-02T09:00:00Z",
    duration_minutes: int = 30,
    summary: str = "Weekly standup",
    count: int | None = None,
    until: str | None = None,
    by_day: str = "MO",
) -> dict:
    """Build a parent recurring-event body."""
    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
    end_dt = start_dt + timedelta(minutes=duration_minutes)
    rrule = f"RRULE:FREQ=WEEKLY;BYDAY={by_day}"
    if count is not None:
        rrule += f";COUNT={count}"
    if until is not None:
        rrule += f";UNTIL={until}"
    return {
        "summary": summary,
        "start": {"dateTime": start, "timeZone": "UTC"},
        "end": {"dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), "timeZone": "UTC"},
        "recurrence": [rrule],
    }


# ---------------------------------------------------------------------------
# derive_instance_event_id helper
# ---------------------------------------------------------------------------
def test_derive_instance_id_for_timed_event():
    parent = "bbparent000001"
    ost = {"dateTime": "2026-02-02T09:00:00Z"}
    assert derive_instance_event_id(parent, ost) == "bbparent000001_20260202T090000Z"


def test_derive_instance_id_for_all_day_event():
    parent = "bbparent000001"
    ost = {"date": "2026-02-02"}
    assert derive_instance_event_id(parent, ost) == "bbparent000001_20260202"


def test_derive_instance_id_with_offset_dt():
    """A non-UTC dateTime is converted to UTC for the suffix."""
    parent = "bbparent000001"
    ost = {"dateTime": "2026-02-02T04:00:00-05:00"}
    assert derive_instance_event_id(parent, ost) == "bbparent000001_20260202T090000Z"


def test_derive_instance_id_raises_on_empty():
    with pytest.raises(ValueError):
        derive_instance_event_id("p", {})


# ---------------------------------------------------------------------------
# events.get on derived instance ID
# ---------------------------------------------------------------------------
def test_get_synthesises_unmodified_instance(fake):
    body = _weekly_body(count=4)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)

    # The third Monday after 2026-02-02 is 2026-02-16.
    inst_id = derive_instance_event_id(
        "bbparent000001",
        {"dateTime": "2026-02-16T09:00:00Z"},
    )
    inst = fake.get_event("primary", inst_id)
    assert inst["id"] == inst_id
    assert inst["recurringEventId"] == "bbparent000001"
    assert inst["originalStartTime"]["dateTime"].startswith("2026-02-16T09:00:00")
    # No override row should have been created.
    assert "bbparent000001" in fake.all_event_ids("primary")
    assert inst_id not in fake.all_event_ids("primary")


def test_get_synthesised_instance_outside_recurrence_404(fake):
    body = _weekly_body(count=4)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)

    # Tuesday is not a Monday occurrence.
    bogus_id = "bbparent000001_20260203T090000Z"
    with pytest.raises(GoogleApiError) as exc_info:
        fake.get_event("primary", bogus_id)
    assert exc_info.value.status == 404


# ---------------------------------------------------------------------------
# Instance modification via update/patch
# ---------------------------------------------------------------------------
def test_update_on_derived_instance_id_materialises_override(fake):
    body = _weekly_body(count=4)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)
    inst_id = "bbparent000001_20260216T090000Z"

    new_body = _weekly_body(start="2026-02-16T11:00:00Z", summary="moved")
    new_body.pop("recurrence", None)  # instance, not series
    out = fake.update_event("primary", inst_id, new_body)
    assert out["id"] == inst_id
    assert out["recurringEventId"] == "bbparent000001"
    assert out["originalStartTime"]["dateTime"].startswith("2026-02-16T09:00:00")
    assert out["start"]["dateTime"] == "2026-02-16T11:00:00Z"
    assert out["summary"] == "moved"

    # The override row is now real.
    assert inst_id in fake.all_event_ids("primary")


def test_patch_on_derived_instance_id_materialises_override(fake):
    body = _weekly_body(count=4)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)
    inst_id = "bbparent000001_20260216T090000Z"

    out = fake.patch_event("primary", inst_id, {"summary": "patched"})
    assert out["id"] == inst_id
    assert out["summary"] == "patched"
    assert out["recurringEventId"] == "bbparent000001"


# ---------------------------------------------------------------------------
# Instance cancellation via delete
# ---------------------------------------------------------------------------
def test_delete_on_derived_instance_id_materialises_cancelled_override(fake):
    body = _weekly_body(count=4)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)
    inst_id = "bbparent000001_20260216T090000Z"
    fake.delete_event("primary", inst_id)
    refetched = fake.get_event("primary", inst_id)
    assert refetched["status"] == "cancelled"
    assert refetched["recurringEventId"] == "bbparent000001"


def test_delete_on_unknown_id_404(fake):
    body = _weekly_body(count=4)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)
    # Tuesday is not a recurrence occurrence.
    with pytest.raises(GoogleApiError) as exc_info:
        fake.delete_event("primary", "bbparent000001_20260203T090000Z")
    assert exc_info.value.status == 404


# ---------------------------------------------------------------------------
# events.instances
# ---------------------------------------------------------------------------
def test_list_instances_returns_synthesised_instances(fake):
    body = _weekly_body(count=3)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)
    out = fake.list_instances("primary", "bbparent000001")
    starts = [i["start"]["dateTime"][:10] for i in out["items"]]
    # Mondays 2026-02-02, 02-09, 02-16
    assert starts == ["2026-02-02", "2026-02-09", "2026-02-16"]


def test_list_instances_uses_overrides_when_present(fake):
    body = _weekly_body(count=3)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)
    inst_id = "bbparent000001_20260209T090000Z"
    fake.patch_event("primary", inst_id, {"summary": "modified middle"})
    out = fake.list_instances("primary", "bbparent000001")
    summaries = {i.get("summary", "Weekly standup") for i in out["items"]}
    assert "modified middle" in summaries


def test_list_instances_show_deleted_includes_cancelled(fake):
    body = _weekly_body(count=4)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)
    fake.delete_event("primary", "bbparent000001_20260216T090000Z")

    visible = fake.list_instances("primary", "bbparent000001")
    assert all(i["status"] == "confirmed" for i in visible["items"])
    assert len(visible["items"]) == 3

    with_cancelled = fake.list_instances(
        "primary", "bbparent000001", show_deleted=True,
    )
    statuses = [i["status"] for i in with_cancelled["items"]]
    assert "cancelled" in statuses
    assert len(with_cancelled["items"]) == 4


def test_list_instances_404_unknown_event(fake):
    with pytest.raises(GoogleApiError) as exc_info:
        fake.list_instances("primary", "bbnone0000001")
    assert exc_info.value.status == 404


def test_list_instances_400_for_non_recurring(fake):
    out = fake.insert_event(
        "primary",
        {
            "id": "bbnonrec000001",
            "summary": "single",
            "start": {"dateTime": "2026-02-02T09:00:00Z", "timeZone": "UTC"},
            "end": {"dateTime": "2026-02-02T09:30:00Z", "timeZone": "UTC"},
        },
    )
    with pytest.raises(GoogleApiError) as exc_info:
        fake.list_instances("primary", out["id"])
    assert exc_info.value.status == 400


# ---------------------------------------------------------------------------
# events.list(singleEvents=True)
# ---------------------------------------------------------------------------
def test_list_single_events_expands_parent(fake):
    body = _weekly_body(count=3)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)
    out = fake.list_events(
        "primary",
        single_events=True,
        time_min="2026-01-01T00:00:00Z",
        time_max="2026-03-01T00:00:00Z",
    )
    starts = sorted(i["start"]["dateTime"][:10] for i in out["items"])
    assert starts == ["2026-02-02", "2026-02-09", "2026-02-16"]


def test_list_single_events_skips_cancelled_overrides_by_default(fake):
    body = _weekly_body(count=4)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)
    fake.delete_event("primary", "bbparent000001_20260216T090000Z")
    out = fake.list_events(
        "primary",
        single_events=True,
        time_min="2026-01-01T00:00:00Z",
        time_max="2026-03-01T00:00:00Z",
    )
    assert len(out["items"]) == 3


# ---------------------------------------------------------------------------
# Full-sync-omits-cancelled-instances QUIRK (the headline bug)
# ---------------------------------------------------------------------------
def test_full_sync_omits_cancelled_instance_overrides_with_show_deleted(fake):
    """The bug: even with showDeleted=True, full sync does NOT
    return cancelled instance overrides.  Production code that asks
    for showDeleted=True still loses track of cancelled instances —
    only ``events.instances(showDeleted=True)`` (or incremental
    sync) reliably surfaces them."""
    body = _weekly_body(count=4)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)
    fake.delete_event("primary", "bbparent000001_20260216T090000Z")

    out = fake.list_events("primary", show_deleted=True)
    ids = {e["id"] for e in out["items"]}
    # Parent IS returned.
    assert "bbparent000001" in ids
    # Cancelled instance override is NOT.
    assert "bbparent000001_20260216T090000Z" not in ids


def test_incremental_sync_does_surface_cancelled_instance(fake):
    """The flip-side: incremental sync sees the cancellation, so
    a sync-token-based ingest path can still notice it (the
    rewrite plan calls this out as one of the reliable paths)."""
    body = _weekly_body(count=4)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)
    sync = fake.list_events("primary")["nextSyncToken"]

    fake.delete_event("primary", "bbparent000001_20260216T090000Z")
    out = fake.list_events("primary", sync_token=sync)
    cancelled = [e for e in out["items"] if e["status"] == "cancelled"]
    assert len(cancelled) == 1
    assert cancelled[0]["id"] == "bbparent000001_20260216T090000Z"
    assert cancelled[0]["recurringEventId"] == "bbparent000001"


def test_instances_endpoint_does_surface_cancelled_with_show_deleted(fake):
    """The reliable retrieval path: events.instances(showDeleted=True)."""
    body = _weekly_body(count=4)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)
    fake.delete_event("primary", "bbparent000001_20260216T090000Z")
    out = fake.list_instances("primary", "bbparent000001", show_deleted=True)
    statuses = [i["status"] for i in out["items"]]
    assert statuses.count("cancelled") == 1


# ---------------------------------------------------------------------------
# _R "this and following" reschedule quirk
# ---------------------------------------------------------------------------
def test_reschedule_creates_R_suffixed_new_series(fake):
    """When the user picks 'this and following' in Google's UI,
    we end up with two parents: the original (truncated) and a new
    one with id ``<parent>_R<stamp>``.  Production has special-case
    code in app/sync/rules.py:82 to detect this."""
    body = _weekly_body(count=10)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)

    new_body = _weekly_body(
        start="2026-02-16T11:00:00Z",
        summary="moved series",
        count=8,
    )
    out = fake.reschedule_series_this_and_following(
        "primary",
        "bbparent000001",
        from_dt="2026-02-16T09:00:00Z",
        new_body=new_body,
    )
    assert out["id"] == "bbparent000001_R20260216T090000Z"
    assert out["recurrence"]  # is itself a recurring series
    assert out["start"]["dateTime"] == "2026-02-16T11:00:00Z"


def test_reschedule_truncates_original_series(fake):
    body = _weekly_body(count=10)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)

    new_body = _weekly_body(
        start="2026-02-16T11:00:00Z",
        summary="moved",
        count=8,
    )
    fake.reschedule_series_this_and_following(
        "primary",
        "bbparent000001",
        from_dt="2026-02-16T09:00:00Z",
        new_body=new_body,
    )
    insts = fake.list_instances("primary", "bbparent000001")
    starts = sorted(i["start"]["dateTime"][:10] for i in insts["items"])
    # Only 2026-02-02 and 2026-02-09 remain in the original series.
    assert starts == ["2026-02-02", "2026-02-09"]


def test_reschedule_cancels_post_boundary_overrides(fake):
    body = _weekly_body(count=10)
    body["id"] = "bbparent000001"
    fake.insert_event("primary", body)

    # Customise the 2026-02-23 instance BEFORE the reschedule.
    fake.patch_event(
        "primary",
        "bbparent000001_20260223T090000Z",
        {"summary": "customised"},
    )

    new_body = _weekly_body(
        start="2026-02-16T11:00:00Z",
        summary="moved",
        count=8,
    )
    fake.reschedule_series_this_and_following(
        "primary",
        "bbparent000001",
        from_dt="2026-02-16T09:00:00Z",
        new_body=new_body,
    )
    # The 2026-02-23 override should now be cancelled (it lived
    # post-boundary in the original series).
    refetched = fake.get_event("primary", "bbparent000001_20260223T090000Z")
    assert refetched["status"] == "cancelled"


def test_reschedule_400_for_non_recurring(fake):
    out = fake.insert_event(
        "primary",
        {
            "id": "bbsingle000001",
            "summary": "single",
            "start": {"dateTime": "2026-02-02T09:00:00Z", "timeZone": "UTC"},
            "end": {"dateTime": "2026-02-02T09:30:00Z", "timeZone": "UTC"},
        },
    )
    with pytest.raises(GoogleApiError) as exc_info:
        fake.reschedule_series_this_and_following(
            "primary", out["id"], "2026-02-09T09:00:00Z", {}
        )
    assert exc_info.value.status == 400


# ---------------------------------------------------------------------------
# All-day recurring
# ---------------------------------------------------------------------------
def test_all_day_recurring_instance_id_format(fake):
    body = {
        "id": "bballda0000001",
        "summary": "Daily all-day",
        "start": {"date": "2026-02-02"},
        "end": {"date": "2026-02-03"},
        "recurrence": ["RRULE:FREQ=DAILY;COUNT=3"],
    }
    fake.insert_event("primary", body)
    inst_id = derive_instance_event_id("bballda0000001", {"date": "2026-02-03"})
    inst = fake.get_event("primary", inst_id)
    assert inst["start"]["date"] == "2026-02-03"
    assert inst["recurringEventId"] == "bballda0000001"
