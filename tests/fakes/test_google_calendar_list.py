"""Tests for ``FakeGoogleCalendar.list_events``: full sync, incremental
sync, pagination, time windows, and sync-token expiry."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.fakes.clock import SimulatedClock
from tests.fakes.google_calendar import (
    FakeGoogleCalendar,
    GoogleApiError,
)

UTC = timezone.utc


@pytest.fixture
def clock() -> SimulatedClock:
    return SimulatedClock()


@pytest.fixture
def fake(clock: SimulatedClock) -> FakeGoogleCalendar:
    g = FakeGoogleCalendar(clock=clock)
    g.add_calendar("primary")
    return g


def _ev(start: str, summary: str = "ev") -> dict:
    """Build an event body with a 30-minute duration.

    ``start`` must be ``YYYY-MM-DDTHH:MM:SSZ``; the end is computed
    by adding 30 minutes.
    """
    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
    end_dt = start_dt + timedelta(minutes=30)
    return {
        "summary": summary,
        "start": {"dateTime": start, "timeZone": "UTC"},
        "end": {
            "dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "timeZone": "UTC",
        },
    }


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# Full sync basics
# ---------------------------------------------------------------------------
def test_full_sync_returns_all_confirmed_events(fake):
    a = fake.insert_event("primary", _ev("2026-02-01T09:00:00Z", "A"))
    b = fake.insert_event("primary", _ev("2026-02-02T09:00:00Z", "B"))
    out = fake.list_events("primary")
    ids = {e["id"] for e in out["items"]}
    assert ids == {a["id"], b["id"]}
    assert "nextSyncToken" in out
    assert "nextPageToken" not in out


def test_full_sync_skips_cancelled_by_default(fake):
    fake.insert_event("primary", _ev("2026-02-01T09:00:00Z", "A"))
    b = fake.insert_event("primary", _ev("2026-02-02T09:00:00Z", "B"))
    fake.delete_event("primary", b["id"])
    out = fake.list_events("primary")
    assert {e["id"] for e in out["items"]} == {
        e["id"] for e in out["items"] if e["status"] == "confirmed"
    }
    assert b["id"] not in {e["id"] for e in out["items"]}


def test_full_sync_show_deleted_includes_cancelled(fake):
    a = fake.insert_event("primary", _ev("2026-02-01T09:00:00Z", "A"))
    fake.delete_event("primary", a["id"])
    out = fake.list_events("primary", show_deleted=True)
    statuses = {e["status"] for e in out["items"]}
    assert "cancelled" in statuses


def test_full_sync_filters_by_time_window(fake):
    fake.insert_event("primary", _ev("2026-01-15T09:00:00Z", "before"))
    inside = fake.insert_event("primary", _ev("2026-02-15T09:00:00Z", "inside"))
    fake.insert_event("primary", _ev("2026-04-15T09:00:00Z", "after"))
    out = fake.list_events(
        "primary",
        time_min=_dt("2026-02-01T00:00:00Z"),
        time_max=_dt("2026-03-01T00:00:00Z"),
    )
    assert {e["id"] for e in out["items"]} == {inside["id"]}


def test_full_sync_accepts_iso_string_for_time_window(fake):
    inside = fake.insert_event("primary", _ev("2026-02-15T09:00:00Z", "inside"))
    fake.insert_event("primary", _ev("2026-04-15T09:00:00Z", "after"))
    out = fake.list_events(
        "primary",
        time_min="2026-02-01T00:00:00Z",
        time_max="2026-03-01T00:00:00Z",
    )
    assert {e["id"] for e in out["items"]} == {inside["id"]}


def test_full_sync_returns_stable_chronological_order(fake):
    later = fake.insert_event("primary", _ev("2026-02-10T09:00:00Z"))
    earlier = fake.insert_event("primary", _ev("2026-02-01T09:00:00Z"))
    middle = fake.insert_event("primary", _ev("2026-02-05T09:00:00Z"))
    out = fake.list_events("primary")
    ids_in_order = [e["id"] for e in out["items"]]
    assert ids_in_order == [earlier["id"], middle["id"], later["id"]]


# ---------------------------------------------------------------------------
# Incremental sync
# ---------------------------------------------------------------------------
def test_incremental_sync_returns_only_changes_since_token(fake):
    a = fake.insert_event("primary", _ev("2026-02-01T09:00:00Z", "A"))
    out1 = fake.list_events("primary")
    sync = out1["nextSyncToken"]

    # Nothing changed yet.
    out2 = fake.list_events("primary", sync_token=sync)
    assert out2["items"] == []

    # Add a new event.
    b = fake.insert_event("primary", _ev("2026-02-02T09:00:00Z", "B"))
    out3 = fake.list_events("primary", sync_token=sync)
    assert {e["id"] for e in out3["items"]} == {b["id"]}


def test_incremental_sync_includes_cancellations(fake):
    a = fake.insert_event("primary", _ev("2026-02-01T09:00:00Z", "A"))
    sync = fake.list_events("primary")["nextSyncToken"]
    fake.delete_event("primary", a["id"])
    out = fake.list_events("primary", sync_token=sync)
    assert len(out["items"]) == 1
    assert out["items"][0]["id"] == a["id"]
    assert out["items"][0]["status"] == "cancelled"


def test_incremental_sync_returns_updated_events(fake):
    a = fake.insert_event("primary", _ev("2026-02-01T09:00:00Z", "A"))
    sync = fake.list_events("primary")["nextSyncToken"]
    fake.update_event("primary", a["id"], _ev("2026-02-01T10:00:00Z", "A!"))
    out = fake.list_events("primary", sync_token=sync)
    assert {e["id"] for e in out["items"]} == {a["id"]}
    assert out["items"][0]["summary"] == "A!"


def test_incremental_sync_chained_tokens_each_advance(fake):
    fake.insert_event("primary", _ev("2026-02-01T09:00:00Z", "A"))
    s1 = fake.list_events("primary")["nextSyncToken"]
    fake.insert_event("primary", _ev("2026-02-02T09:00:00Z", "B"))
    s2 = fake.list_events("primary", sync_token=s1)["nextSyncToken"]
    # No more changes.
    out3 = fake.list_events("primary", sync_token=s2)
    assert out3["items"] == []
    assert "nextSyncToken" in out3


def test_incremental_sync_with_time_min_is_a_bad_request(fake):
    sync = fake.list_events("primary")["nextSyncToken"]
    with pytest.raises(GoogleApiError) as exc_info:
        fake.list_events(
            "primary",
            sync_token=sync,
            time_min="2026-01-01T00:00:00Z",
        )
    assert exc_info.value.status == 400


def test_incremental_with_unknown_token_410(fake):
    with pytest.raises(GoogleApiError) as exc_info:
        fake.list_events("primary", sync_token="sync-bogus")
    assert exc_info.value.status == 410


# ---------------------------------------------------------------------------
# Sync token expiry
# ---------------------------------------------------------------------------
def test_sync_token_expires_after_ttl(fake, clock):
    sync = fake.list_events("primary")["nextSyncToken"]
    # TTL default = 30d, advance past it.
    clock.advance(timedelta(days=30, seconds=1))
    with pytest.raises(GoogleApiError) as exc_info:
        fake.list_events("primary", sync_token=sync)
    assert exc_info.value.status == 410


def test_sync_token_still_valid_just_under_ttl(fake, clock):
    sync = fake.list_events("primary")["nextSyncToken"]
    clock.advance(timedelta(days=29, hours=23))
    out = fake.list_events("primary", sync_token=sync)
    assert "nextSyncToken" in out


def test_expire_sync_token_helper(fake):
    sync = fake.list_events("primary")["nextSyncToken"]
    fake.expire_sync_token(sync)
    with pytest.raises(GoogleApiError) as exc_info:
        fake.list_events("primary", sync_token=sync)
    assert exc_info.value.status == 410


def test_custom_ttl_honoured(clock):
    g = FakeGoogleCalendar(clock=clock, sync_token_ttl=timedelta(hours=1))
    g.add_calendar("primary")
    sync = g.list_events("primary")["nextSyncToken"]
    clock.advance(timedelta(hours=2))
    with pytest.raises(GoogleApiError):
        g.list_events("primary", sync_token=sync)


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------
def test_pagination_splits_results(fake):
    ids = []
    for i in range(7):
        out = fake.insert_event(
            "primary", _ev(f"2026-02-{i+1:02d}T09:00:00Z", f"e{i}")
        )
        ids.append(out["id"])

    page1 = fake.list_events("primary", max_results=3)
    assert len(page1["items"]) == 3
    assert "nextPageToken" in page1
    assert "nextSyncToken" not in page1

    page2 = fake.list_events("primary", page_token=page1["nextPageToken"])
    assert len(page2["items"]) == 3
    assert "nextPageToken" in page2
    assert "nextSyncToken" not in page2

    page3 = fake.list_events("primary", page_token=page2["nextPageToken"])
    assert len(page3["items"]) == 1
    assert "nextPageToken" not in page3
    assert "nextSyncToken" in page3

    # All seven events appear exactly once across the three pages.
    seen = (
        {e["id"] for e in page1["items"]}
        | {e["id"] for e in page2["items"]}
        | {e["id"] for e in page3["items"]}
    )
    assert seen == set(ids)


def test_sync_token_after_pagination_reflects_snapshot_cursor(fake):
    """Events created mid-pagination still appear in the next incremental."""
    for i in range(4):
        fake.insert_event("primary", _ev(f"2026-02-{i+1:02d}T09:00:00Z"))
    page1 = fake.list_events("primary", max_results=2)

    # Insert a new event between pages.
    new = fake.insert_event("primary", _ev("2026-02-20T09:00:00Z", "MidPage"))

    page2 = fake.list_events("primary", page_token=page1["nextPageToken"])
    sync = page2["nextSyncToken"]
    # The mid-page event should NOT be in the original snapshot.
    assert new["id"] not in {e["id"] for e in page2["items"]}

    # …but it WILL show up in the next incremental sync.
    out = fake.list_events("primary", sync_token=sync)
    assert new["id"] in {e["id"] for e in out["items"]}


def test_unknown_page_token_410(fake):
    with pytest.raises(GoogleApiError) as exc_info:
        fake.list_events("primary", page_token="page-bogus")
    assert exc_info.value.status == 410


def test_page_token_consumed_after_use(fake):
    for i in range(5):
        fake.insert_event("primary", _ev(f"2026-02-{i+1:02d}T09:00:00Z"))
    page1 = fake.list_events("primary", max_results=2)
    fake.list_events("primary", page_token=page1["nextPageToken"])
    with pytest.raises(GoogleApiError):
        fake.list_events("primary", page_token=page1["nextPageToken"])


def test_max_results_must_be_positive(fake):
    with pytest.raises(GoogleApiError) as exc_info:
        fake.list_events("primary", max_results=0)
    assert exc_info.value.status == 400


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------
def test_empty_calendar_returns_empty_items_with_sync_token(fake):
    out = fake.list_events("primary")
    assert out["items"] == []
    assert "nextSyncToken" in out


def test_unknown_calendar_404_on_list(fake):
    with pytest.raises(GoogleApiError) as exc_info:
        fake.list_events("nope")
    assert exc_info.value.status == 404


def test_single_events_true_not_yet_implemented(fake):
    """Recurring expansion lands in the next commit; tests using it
    should fail loudly until then."""
    fake.insert_event("primary", _ev("2026-02-01T09:00:00Z"))
    with pytest.raises(NotImplementedError):
        fake.list_events("primary", single_events=True)
