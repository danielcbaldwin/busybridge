"""Tests for the core CRUD / ETag / client-id semantics of the fake Google."""

from __future__ import annotations

import pytest

from tests.fakes.clock import SimulatedClock
from tests.fakes.google_calendar import (
    FakeGoogleCalendar,
    GoogleApiError,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def clock() -> SimulatedClock:
    return SimulatedClock()


@pytest.fixture
def fake(clock: SimulatedClock) -> FakeGoogleCalendar:
    g = FakeGoogleCalendar(clock=clock)
    g.add_calendar("primary", "Main calendar", time_zone="UTC")
    g.add_calendar("client_a@group.calendar.google.com", "Client A")
    return g


def _basic_body(summary: str = "Standup") -> dict:
    return {
        "summary": summary,
        "start": {"dateTime": "2026-02-01T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-02-01T09:30:00Z", "timeZone": "UTC"},
    }


# ---------------------------------------------------------------------------
# Calendar lifecycle
# ---------------------------------------------------------------------------
def test_list_calendars_includes_added(fake):
    items = fake.list_calendars()["items"]
    ids = {c["id"] for c in items}
    assert ids == {"primary", "client_a@group.calendar.google.com"}


def test_get_calendar_returns_metadata(fake):
    cal = fake.get_calendar("primary")
    assert cal["id"] == "primary"
    assert cal["summary"] == "Main calendar"
    assert cal["timeZone"] == "UTC"


def test_get_calendar_404(fake):
    with pytest.raises(GoogleApiError) as exc_info:
        fake.get_calendar("nope")
    assert exc_info.value.status == 404


def test_add_calendar_duplicate_409(fake):
    with pytest.raises(GoogleApiError) as exc_info:
        fake.add_calendar("primary")
    assert exc_info.value.status == 409


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------
def test_insert_assigns_id_and_etag_when_none_supplied(fake):
    out = fake.insert_event("primary", _basic_body())
    assert out["id"]
    assert out["etag"].startswith('"')
    assert out["status"] == "confirmed"
    assert out["summary"] == "Standup"
    assert out["sequence"] == 0


def test_insert_returns_created_and_updated_timestamps(fake, clock):
    clock.set_time(clock.now())  # no-op, just to anchor
    out = fake.insert_event("primary", _basic_body())
    assert out["created"] == out["updated"]
    # Roughly ISO-8601 UTC with millisecond + Z.
    assert out["created"].endswith("Z")
    assert "T" in out["created"]


def test_insert_with_client_supplied_id(fake):
    body = _basic_body()
    body["id"] = "bb000000000001"  # base32hex, lowercase, > 5 chars
    out = fake.insert_event("primary", body)
    assert out["id"] == "bb000000000001"


def test_insert_client_id_conflict_409(fake):
    body = _basic_body()
    body["id"] = "bbduplicate0000"
    fake.insert_event("primary", body)
    with pytest.raises(GoogleApiError) as exc_info:
        fake.insert_event("primary", body)
    assert exc_info.value.status == 409


def test_insert_client_id_conflict_after_delete_still_409(fake):
    """Cancelled rows still occupy their ID, just like real Google."""
    body = _basic_body()
    body["id"] = "bbdeletedcollide"
    out = fake.insert_event("primary", body)
    fake.delete_event("primary", out["id"])
    with pytest.raises(GoogleApiError) as exc_info:
        fake.insert_event("primary", body)
    assert exc_info.value.status == 409


def test_insert_client_id_invalid_alphabet(fake):
    body = _basic_body()
    body["id"] = "BAD-UPPERCASE!"
    with pytest.raises(GoogleApiError) as exc_info:
        fake.insert_event("primary", body)
    assert exc_info.value.status == 400


def test_insert_client_id_too_short(fake):
    body = _basic_body()
    body["id"] = "abc"
    with pytest.raises(GoogleApiError) as exc_info:
        fake.insert_event("primary", body)
    assert exc_info.value.status == 400


def test_insert_unknown_calendar_404(fake):
    with pytest.raises(GoogleApiError) as exc_info:
        fake.insert_event("nope", _basic_body())
    assert exc_info.value.status == 404


def test_insert_preserves_extended_properties(fake):
    body = _basic_body()
    body["extendedProperties"] = {
        "private": {"bb_proj_id": "42", "bb_origin_id": "src1"},
        "shared": {"team": "platform"},
    }
    out = fake.insert_event("primary", body)
    assert out["extendedProperties"]["private"]["bb_proj_id"] == "42"
    assert out["extendedProperties"]["shared"]["team"] == "platform"


# ---------------------------------------------------------------------------
# Get
# ---------------------------------------------------------------------------
def test_get_event(fake):
    body = _basic_body()
    body["id"] = "bbget0000000001"
    fake.insert_event("primary", body)
    out = fake.get_event("primary", "bbget0000000001")
    assert out["summary"] == "Standup"


def test_get_event_404(fake):
    with pytest.raises(GoogleApiError) as exc_info:
        fake.get_event("primary", "bbnone0000000")
    assert exc_info.value.status == 404


def test_get_event_returns_a_copy_not_a_reference(fake):
    """Mutating the returned dict must not alter stored state."""
    out = fake.insert_event("primary", _basic_body())
    out["summary"] = "MUTATED"
    out["extendedProperties"] = {"private": {"x": "y"}}
    refetched = fake.get_event("primary", out["id"])
    assert refetched["summary"] == "Standup"
    assert "extendedProperties" not in refetched


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------
def test_update_replaces_fields_and_bumps_sequence_and_etag(fake):
    out = fake.insert_event("primary", _basic_body())
    initial_etag = out["etag"]

    body = _basic_body("Standup updated")
    updated = fake.update_event("primary", out["id"], body)
    assert updated["summary"] == "Standup updated"
    assert updated["sequence"] == 1
    assert updated["etag"] != initial_etag


def test_update_with_matching_if_match_succeeds(fake):
    out = fake.insert_event("primary", _basic_body())
    body = _basic_body("Updated")
    updated = fake.update_event(
        "primary", out["id"], body, if_match=out["etag"]
    )
    assert updated["summary"] == "Updated"


def test_update_with_wildcard_if_match_succeeds(fake):
    out = fake.insert_event("primary", _basic_body())
    body = _basic_body("Updated")
    updated = fake.update_event("primary", out["id"], body, if_match="*")
    assert updated["summary"] == "Updated"


def test_update_with_stale_if_match_412(fake):
    out = fake.insert_event("primary", _basic_body())
    # Someone else updates first; etag changes.
    fake.update_event("primary", out["id"], _basic_body("Race"))
    body = _basic_body("Late")
    with pytest.raises(GoogleApiError) as exc_info:
        fake.update_event("primary", out["id"], body, if_match=out["etag"])
    assert exc_info.value.status == 412


def test_update_unknown_event_404(fake):
    with pytest.raises(GoogleApiError) as exc_info:
        fake.update_event("primary", "bbnone0000000", _basic_body())
    assert exc_info.value.status == 404


# ---------------------------------------------------------------------------
# Patch
# ---------------------------------------------------------------------------
def test_patch_only_replaces_supplied_fields(fake):
    body = _basic_body()
    body["description"] = "original"
    out = fake.insert_event("primary", body)
    fake.patch_event("primary", out["id"], {"summary": "Renamed"})
    refetched = fake.get_event("primary", out["id"])
    assert refetched["summary"] == "Renamed"
    assert refetched["description"] == "original"


def test_patch_extended_properties_deep_merges(fake):
    body = _basic_body()
    body["extendedProperties"] = {"private": {"a": "1", "b": "2"}}
    out = fake.insert_event("primary", body)
    fake.patch_event(
        "primary", out["id"],
        {"extendedProperties": {"private": {"b": "two", "c": "3"}}},
    )
    refetched = fake.get_event("primary", out["id"])
    assert refetched["extendedProperties"]["private"] == {
        "a": "1", "b": "two", "c": "3",
    }


def test_patch_with_stale_if_match_412(fake):
    out = fake.insert_event("primary", _basic_body())
    fake.update_event("primary", out["id"], _basic_body("Race"))
    with pytest.raises(GoogleApiError) as exc_info:
        fake.patch_event("primary", out["id"], {"summary": "Late"}, if_match=out["etag"])
    assert exc_info.value.status == 412


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------
def test_delete_marks_cancelled_and_bumps_etag(fake):
    out = fake.insert_event("primary", _basic_body())
    fake.delete_event("primary", out["id"])
    refetched = fake.get_event("primary", out["id"])
    assert refetched["status"] == "cancelled"
    assert refetched["etag"] != out["etag"]


def test_delete_idempotent(fake):
    out = fake.insert_event("primary", _basic_body())
    fake.delete_event("primary", out["id"])
    # Second delete is a no-op (does not raise).
    fake.delete_event("primary", out["id"])


def test_delete_unknown_event_404(fake):
    with pytest.raises(GoogleApiError) as exc_info:
        fake.delete_event("primary", "bbnone0000000")
    assert exc_info.value.status == 404


def test_delete_with_stale_if_match_412(fake):
    out = fake.insert_event("primary", _basic_body())
    fake.update_event("primary", out["id"], _basic_body("Race"))
    with pytest.raises(GoogleApiError) as exc_info:
        fake.delete_event("primary", out["id"], if_match=out["etag"])
    assert exc_info.value.status == 412


def test_get_after_delete_returns_cancelled_status(fake):
    out = fake.insert_event("primary", _basic_body())
    fake.delete_event("primary", out["id"])
    refetched = fake.get_event("primary", out["id"])
    assert refetched["status"] == "cancelled"


def test_update_on_cancelled_event_404(fake):
    """Real Google: once cancelled, updates 404.  Re-create with new ID."""
    out = fake.insert_event("primary", _basic_body())
    fake.delete_event("primary", out["id"])
    with pytest.raises(GoogleApiError) as exc_info:
        fake.update_event("primary", out["id"], _basic_body("revive"))
    assert exc_info.value.status == 404
