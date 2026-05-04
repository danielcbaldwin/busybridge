"""Tests for sync rules."""

import pytest

from app.config import get_settings
from app.sync.google_calendar import (
    create_busy_block,
    copy_event_for_main,
    should_create_busy_block,
    can_user_edit_event,
    derive_instance_event_id,
)


def test_create_busy_block_timed():
    """Test creating a timed busy block."""
    start = {"dateTime": "2024-01-15T10:00:00Z"}
    end = {"dateTime": "2024-01-15T11:00:00Z"}

    block = create_busy_block(start, end, is_all_day=False)

    expected = f"\U0001f510 {get_settings().managed_event_prefix} Busy".strip()
    assert block["summary"] == expected
    assert block["description"] == ""
    assert block["visibility"] == "private"
    assert block["transparency"] == "opaque"
    assert "dateTime" in block["start"]
    assert "dateTime" in block["end"]


def test_create_busy_block_all_day():
    """Test creating an all-day busy block."""
    start = {"date": "2024-01-15"}
    end = {"date": "2024-01-16"}

    block = create_busy_block(start, end, is_all_day=True)

    expected = f"\U0001f510 {get_settings().managed_event_prefix} Busy".strip()
    assert block["summary"] == expected
    assert "date" in block["start"]
    assert "date" in block["end"]
    assert "dateTime" not in block["start"]


def test_copy_event_for_main():
    """Test copying an event for main calendar."""
    source = {
        "summary": "Client Meeting",
        "description": "Discuss project timeline",
        "location": "Conference Room A",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": [
            {"email": "client@example.com"},
            {"email": "colleague@example.com"},
        ],
    }

    result = copy_event_for_main(source, source_label="Client A (client@example.com)")

    assert result["summary"] == "Client Meeting"
    assert result["location"] == "Conference Room A"
    desc = result["description"]
    assert desc.startswith("Discuss project timeline")
    assert "Source: Client A (client@example.com)" in desc
    # New rich attendee block replaces the old "Original attendees:" line.
    assert "Attendees (2):" in desc
    assert "Emails: client@example.com, colleague@example.com" in desc
    assert "Original attendees:" not in desc


def _attendee_section(desc: str) -> str:
    """Slice the description down to the attendee block for easier asserts."""
    start = desc.find("Attendees (")
    if start == -1:
        return ""
    # Section ends before the trailer (Source / Original event) or end of string.
    for marker in ("\n\nSource:", "\n\nOriginal event:"):
        idx = desc.find(marker, start)
        if idx != -1:
            return desc[start:idx]
    return desc[start:]


def test_attendee_block_basic_accepted():
    """Single accepted attendee — icon only, no status word, name in list, email in Emails line."""
    source = {
        "summary": "Sync",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": [
            {"email": "alice@acme.com", "displayName": "Alice Chen", "responseStatus": "accepted"},
        ],
    }
    desc = copy_event_for_main(source)["description"]
    section = _attendee_section(desc)
    assert "Attendees (1): 1 yes, 0 no, 0 maybe, 0 pending" in section
    assert "✅ Alice Chen" in section
    assert "— accepted" not in section  # no status word for accepted
    assert "Emails: alice@acme.com" in section


def test_attendee_block_all_statuses():
    """One of each responseStatus → all four icons + correct status words."""
    source = {
        "summary": "Mixed",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": [
            {"email": "a@x.com", "displayName": "Alice", "responseStatus": "accepted"},
            {"email": "b@x.com", "displayName": "Bob", "responseStatus": "declined"},
            {"email": "c@x.com", "displayName": "Carol", "responseStatus": "tentative"},
            {"email": "d@x.com", "displayName": "Dan", "responseStatus": "needsAction"},
        ],
    }
    section = _attendee_section(copy_event_for_main(source)["description"])
    assert "✅ Alice" in section
    assert "❌ Bob — declined" in section
    assert "❓ Carol — tentative" in section
    assert "⏳ Dan — no response" in section


def test_attendee_block_summary_counts():
    """Summary line counts mixed statuses correctly."""
    source = {
        "summary": "Big",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": [
            {"email": "a@x.com", "responseStatus": "accepted"},
            {"email": "b@x.com", "responseStatus": "accepted"},
            {"email": "c@x.com", "responseStatus": "accepted"},
            {"email": "d@x.com", "responseStatus": "declined"},
            {"email": "e@x.com", "responseStatus": "tentative"},
            {"email": "f@x.com", "responseStatus": "tentative"},
            {"email": "g@x.com", "responseStatus": "needsAction"},
        ],
    }
    section = _attendee_section(copy_event_for_main(source)["description"])
    assert "Attendees (7): 3 yes, 1 no, 2 maybe, 1 pending" in section


def test_attendee_block_name_fallback():
    """displayName missing → Title-Cased local-part in icon list, full email in Emails line."""
    source = {
        "summary": "List",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": [
            {"email": "eng-team@acme.com", "responseStatus": "accepted"},
            {"email": "john.doe@acme.com", "responseStatus": "tentative"},
        ],
    }
    section = _attendee_section(copy_event_for_main(source)["description"])
    assert "✅ Eng Team" in section
    assert "❓ John Doe — tentative" in section
    assert "Emails: eng-team@acme.com, john.doe@acme.com" in section


def test_attendee_block_strips_self_email():
    """Attendee whose email matches main_email (case-insensitive) is omitted."""
    source = {
        "summary": "Self",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": [
            {"email": "Me@Home.com", "displayName": "Me", "responseStatus": "accepted"},
            {"email": "alice@acme.com", "displayName": "Alice", "responseStatus": "accepted"},
        ],
    }
    section = _attendee_section(
        copy_event_for_main(source, main_email="me@home.com")["description"]
    )
    assert "Attendees (1):" in section
    assert "Alice" in section
    assert "Me@Home.com" not in section
    assert "me@home.com" not in section


def test_attendee_block_strips_self_flag():
    """Attendee with self: true is omitted regardless of email match."""
    source = {
        "summary": "SelfFlag",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": [
            {"email": "ghost@x.com", "self": True, "responseStatus": "accepted"},
            {"email": "alice@acme.com", "displayName": "Alice", "responseStatus": "accepted"},
        ],
    }
    section = _attendee_section(copy_event_for_main(source)["description"])
    assert "Attendees (1):" in section
    assert "ghost@x.com" not in section


def test_attendee_block_strips_resource():
    """Resource attendees (rooms/equipment) are omitted."""
    source = {
        "summary": "Resource",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": [
            {"email": "room-7@acme.com", "displayName": "Room 7", "resource": True, "responseStatus": "accepted"},
            {"email": "alice@acme.com", "displayName": "Alice", "responseStatus": "accepted"},
        ],
    }
    section = _attendee_section(copy_event_for_main(source)["description"])
    assert "Attendees (1):" in section
    assert "Room 7" not in section
    assert "room-7@acme.com" not in section


def test_attendee_block_organizer_flag():
    """organizer: true renders ' (organizer)' after the name."""
    source = {
        "summary": "Org",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": [
            {"email": "alice@acme.com", "displayName": "Alice Chen", "organizer": True, "responseStatus": "accepted"},
        ],
    }
    section = _attendee_section(copy_event_for_main(source)["description"])
    assert "✅ Alice Chen (organizer)" in section


def test_attendee_block_optional_flag():
    """optional: true renders ' (optional)' at the end of the line."""
    source = {
        "summary": "Opt",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": [
            {"email": "alice@acme.com", "displayName": "Alice", "optional": True, "responseStatus": "tentative"},
        ],
    }
    section = _attendee_section(copy_event_for_main(source)["description"])
    assert "❓ Alice — tentative (optional)" in section


def test_attendee_block_organizer_plus_status():
    """Organizer + non-accepted status: ' (organizer) — <status>'."""
    source = {
        "summary": "OrgT",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": [
            {"email": "alice@acme.com", "displayName": "Alice", "organizer": True, "responseStatus": "tentative"},
        ],
    }
    section = _attendee_section(copy_event_for_main(source)["description"])
    assert "❓ Alice (organizer) — tentative" in section


def test_attendee_block_long_list_truncation():
    """20 attendees → icon list shows 15 + truncation note; Emails line shows all 20."""
    attendees = [
        {"email": f"user{i:02d}@acme.com", "displayName": f"User {i:02d}", "responseStatus": "accepted"}
        for i in range(20)
    ]
    source = {
        "summary": "Big",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": attendees,
    }
    section = _attendee_section(copy_event_for_main(source)["description"])
    assert "Attendees (20):" in section
    assert "✅ User 00" in section
    assert "✅ User 14" in section
    assert "✅ User 15" not in section  # truncated
    assert "… and 5 more on the original event" in section
    # Emails line lists all 20
    for i in range(20):
        assert f"user{i:02d}@acme.com" in section


def test_attendee_block_no_attendees_omits_section():
    """Source event with no attendees → no Attendees block, footer falls back."""
    source = {
        "summary": "Solo",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
    }
    desc = copy_event_for_main(source, source_label="My Cal")["description"]
    assert "Attendees (" not in desc
    assert "Emails:" not in desc
    assert "Source: My Cal" in desc


def test_attendee_block_all_filtered_omits_section():
    """Only attendee is self → after filtering, block is omitted."""
    source = {
        "summary": "OnlyMe",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": [
            {"email": "me@home.com", "self": True, "responseStatus": "accepted"},
        ],
    }
    desc = copy_event_for_main(source, main_email="me@home.com")["description"]
    assert "Attendees (" not in desc
    assert "Emails:" not in desc


def test_attendee_block_missing_status_treated_as_pending():
    """Attendee without responseStatus → counted as pending, ⏳, 'no response'."""
    source = {
        "summary": "Missing",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": [
            {"email": "alice@acme.com", "displayName": "Alice"},
        ],
    }
    section = _attendee_section(copy_event_for_main(source)["description"])
    assert "Attendees (1): 0 yes, 0 no, 0 maybe, 1 pending" in section
    assert "⏳ Alice — no response" in section


def test_attendee_block_order_preserved():
    """Emails line order matches icon list order (which matches source order)."""
    source = {
        "summary": "Order",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "attendees": [
            {"email": "zach@x.com", "displayName": "Zach", "responseStatus": "accepted"},
            {"email": "alice@x.com", "displayName": "Alice", "responseStatus": "accepted"},
            {"email": "mike@x.com", "displayName": "Mike", "responseStatus": "accepted"},
        ],
    }
    section = _attendee_section(copy_event_for_main(source)["description"])
    assert "Emails: zach@x.com, alice@x.com, mike@x.com" in section
    # Icon order matches: Zach appears before Alice appears before Mike
    z_idx = section.find("✅ Zach")
    a_idx = section.find("✅ Alice")
    m_idx = section.find("✅ Mike")
    assert 0 <= z_idx < a_idx < m_idx


def test_copy_event_for_main_with_recurrence():
    """Test copying a recurring event."""
    source = {
        "summary": "Weekly Standup",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T10:30:00Z"},
        "recurrence": ["RRULE:FREQ=WEEKLY;BYDAY=MO"],
    }

    result = copy_event_for_main(source)

    assert "recurrence" in result
    assert result["recurrence"] == ["RRULE:FREQ=WEEKLY;BYDAY=MO"]
    assert result["summary"] == f"{get_settings().managed_event_prefix} Weekly Standup".strip()


def test_should_create_busy_block_normal_event():
    """Test busy block decision for normal event."""
    event = {
        "id": "event1",
        "status": "confirmed",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
    }

    assert should_create_busy_block(event) is True


def test_should_create_busy_block_cancelled():
    """Test busy block decision for cancelled event."""
    event = {
        "id": "event1",
        "status": "cancelled",
    }

    assert should_create_busy_block(event) is False


def test_should_create_busy_block_declined():
    """Test busy block decision for declined event."""
    event = {
        "id": "event1",
        "status": "confirmed",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "attendees": [
            {"email": "me@example.com", "self": True, "responseStatus": "declined"},
        ],
    }

    assert should_create_busy_block(event) is False


def test_should_create_busy_block_all_day_free():
    """Test busy block decision for free all-day event."""
    event = {
        "id": "event1",
        "status": "confirmed",
        "start": {"date": "2024-01-15"},
        "end": {"date": "2024-01-16"},
        "transparency": "transparent",  # Free
    }

    assert should_create_busy_block(event) is False


def test_should_create_busy_block_all_day_busy():
    """Test busy block decision for busy all-day event."""
    event = {
        "id": "event1",
        "status": "confirmed",
        "start": {"date": "2024-01-15"},
        "end": {"date": "2024-01-16"},
        "transparency": "opaque",  # Busy
    }

    assert should_create_busy_block(event) is True


def test_can_user_edit_event_organizer():
    """Test edit permission when user is organizer."""
    event = {
        "organizer": {"email": "me@example.com"},
    }

    assert can_user_edit_event(event, "me@example.com") is True


def test_can_user_edit_event_organizer_self():
    """Test edit permission when user is marked as self organizer."""
    event = {
        "organizer": {"email": "me@example.com", "self": True},
    }

    assert can_user_edit_event(event, "other@example.com") is True


def test_can_user_edit_event_creator():
    """Test edit permission when user is creator."""
    event = {
        "organizer": {"email": "other@example.com"},
        "creator": {"email": "me@example.com"},
    }

    assert can_user_edit_event(event, "me@example.com") is True


def test_can_user_edit_event_guests_can_modify():
    """Test edit permission when guestsCanModify is true."""
    event = {
        "organizer": {"email": "other@example.com"},
        "guestsCanModify": True,
    }

    assert can_user_edit_event(event, "me@example.com") is True


def test_can_user_edit_event_no_permission():
    """Test no edit permission."""
    event = {
        "organizer": {"email": "other@example.com"},
        "creator": {"email": "other@example.com"},
    }

    assert can_user_edit_event(event, "me@example.com") is False


def test_copy_event_for_main_with_color():
    """Test copying an event with a color ID for calendar color-coding."""
    source = {
        "summary": "Client Meeting",
        "description": "Discuss project",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
    }

    result = copy_event_for_main(source, source_label="Client A", color_id="7")

    assert result["colorId"] == "7"
    assert "Client Meeting" in result["summary"]


def test_copy_event_for_main_without_color():
    """Test copying an event without a color ID omits colorId field."""
    source = {
        "summary": "Team Sync",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
    }

    result = copy_event_for_main(source)

    assert "colorId" not in result


def test_copy_event_for_main_with_none_color():
    """Test that passing color_id=None doesn't add colorId."""
    source = {
        "summary": "Standup",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T10:30:00Z"},
    }

    result = copy_event_for_main(source, color_id=None)

    assert "colorId" not in result


# ---------------------------------------------------------------------------
# create_busy_block – DST / timezone handling
# ---------------------------------------------------------------------------

def test_create_busy_block_named_timezone_preserved():
    """Named timeZone in source is passed through unchanged."""
    start = {"dateTime": "2026-01-05T10:00:00-05:00", "timeZone": "America/New_York"}
    end = {"dateTime": "2026-01-05T11:00:00-05:00", "timeZone": "America/New_York"}

    block = create_busy_block(start, end, is_all_day=False)

    assert block["start"]["timeZone"] == "America/New_York"
    assert block["end"]["timeZone"] == "America/New_York"


def test_create_busy_block_utc_suffix_gets_utc_timezone():
    """A dateTime ending in 'Z' should produce timeZone='UTC'."""
    start = {"dateTime": "2026-03-10T15:00:00Z"}
    end = {"dateTime": "2026-03-10T16:00:00Z"}

    block = create_busy_block(start, end, is_all_day=False)

    assert block["start"]["timeZone"] == "UTC"
    assert block["end"]["timeZone"] == "UTC"


def test_create_busy_block_fixed_offset_omits_timezone():
    """A dateTime with a fixed offset but no named timeZone must NOT default to
    'UTC'.  Doing so would cause recurring busy blocks to drift by an hour after
    a DST transition because Google Calendar anchors the RRULE to UTC wall time.
    """
    start = {"dateTime": "2026-01-05T10:00:00-05:00"}
    end = {"dateTime": "2026-01-05T11:00:00-05:00"}

    block = create_busy_block(start, end, is_all_day=False)

    # timeZone must be absent so the fixed offset in dateTime is honoured
    assert "timeZone" not in block["start"]
    assert "timeZone" not in block["end"]
    # But the dateTime itself must be preserved
    assert block["start"]["dateTime"] == "2026-01-05T10:00:00-05:00"
    assert block["end"]["dateTime"] == "2026-01-05T11:00:00-05:00"


# ---------------------------------------------------------------------------
# derive_instance_event_id
# ---------------------------------------------------------------------------

def test_derive_instance_event_id_utc():
    """UTC originalStartTime should produce a …Z suffix instance ID."""
    original_start_time = {"dateTime": "2026-02-27T15:00:00Z"}
    result = derive_instance_event_id("abc123", original_start_time)
    assert result == "abc123_20260227T150000Z"


def test_derive_instance_event_id_with_negative_offset():
    """dateTime with a negative UTC offset should be converted to UTC."""
    # 2026-02-27 10:00 EST = 2026-02-27 15:00 UTC
    original_start_time = {"dateTime": "2026-02-27T10:00:00-05:00"}
    result = derive_instance_event_id("rec456", original_start_time)
    assert result == "rec456_20260227T150000Z"


def test_derive_instance_event_id_with_positive_offset():
    """dateTime with a positive UTC offset should be converted to UTC."""
    # 2026-03-01 12:00 CET (UTC+1) = 2026-03-01 11:00 UTC
    original_start_time = {"dateTime": "2026-03-01T12:00:00+01:00"}
    result = derive_instance_event_id("evt789", original_start_time)
    assert result == "evt789_20260301T110000Z"


def test_derive_instance_event_id_all_day():
    """All-day events should produce a YYYYMMDD suffix."""
    original_start_time = {"date": "2026-04-10"}
    result = derive_instance_event_id("allday001", original_start_time)
    assert result == "allday001_20260410"


def test_derive_instance_event_id_missing_fields_raises():
    """An empty originalStartTime dict should raise ValueError."""
    with pytest.raises(ValueError):
        derive_instance_event_id("parent", {})
