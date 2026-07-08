"""Tests for app/sync/ics_export.py — ICS generation and backup ZIPs."""

from __future__ import annotations

import os
import zipfile
from typing import Optional

import pytest

from app.database import get_database
from app.sync.ics_export import (
    _clean_timestamp,
    _escape_ics,
    _event_to_vevent,
    _events_to_ics,
    _format_dt,
    _format_dt_value,
    _quote_param,
    _unique_entry_name,
)


# ---------------------------------------------------------------------------
# Helpers shared across tests
# ---------------------------------------------------------------------------


async def _insert_user(
    email: str,
    google_user_id: str,
    main_calendar_id: Optional[str] = "main-cal",
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, main_calendar_id)
           VALUES (?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, email.split("@")[0], main_calendar_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


def _timed_event(**overrides) -> dict:
    event = {
        "id": "evt1",
        "summary": "Meeting",
        "status": "confirmed",
        "start": {"dateTime": "2026-07-07T10:00:00Z"},
        "end": {"dateTime": "2026-07-07T11:00:00Z"},
    }
    event.update(overrides)
    return event


# ---------------------------------------------------------------------------
# _format_dt / _format_dt_value
# ---------------------------------------------------------------------------


class TestFormatDt:
    def test_utc_datetime(self):
        line = _format_dt({"dateTime": "2026-07-07T14:00:00Z"}, "DTSTART")
        assert line == "DTSTART:20260707T140000Z"

    def test_offset_without_timezone_converts_to_utc(self):
        # Google returns e.g. -04:00 with no timeZone for single events.
        # Stripping the offset and relabelling Z would be wrong by 4 hours.
        line = _format_dt({"dateTime": "2026-07-07T10:00:00-04:00"}, "DTSTART")
        assert line == "DTSTART:20260707T140000Z"

    def test_positive_offset_without_timezone_converts_to_utc(self):
        line = _format_dt({"dateTime": "2026-07-07T10:00:00+02:00"}, "DTEND")
        assert line == "DTEND:20260707T080000Z"

    def test_offset_with_named_timezone_keeps_wall_time(self):
        line = _format_dt(
            {"dateTime": "2026-07-07T10:00:00-04:00", "timeZone": "America/New_York"},
            "DTSTART",
        )
        assert line == "DTSTART;TZID=America/New_York:20260707T100000"

    def test_all_day_date(self):
        line = _format_dt({"date": "2026-07-07"}, "DTSTART")
        assert line == "DTSTART;VALUE=DATE:20260707"

    def test_fractional_seconds_stripped(self):
        line = _format_dt({"dateTime": "2026-07-07T10:00:00.123Z"}, "DTSTART")
        assert line == "DTSTART:20260707T100000Z"

    def test_empty_dict_returns_none(self):
        assert _format_dt({}, "DTSTART") is None


class TestFormatDtValue:
    def test_all_day_gets_value_date_param(self):
        assert _format_dt_value({"date": "2026-07-07"}) == (";VALUE=DATE", "20260707")

    def test_utc_datetime_has_no_params(self):
        assert _format_dt_value({"dateTime": "2026-07-07T14:00:00Z"}) == (
            "", "20260707T140000Z",
        )

    def test_named_timezone_goes_into_params(self):
        params, value = _format_dt_value(
            {"dateTime": "2026-07-14T10:00:00-04:00", "timeZone": "America/New_York"}
        )
        assert params == ";TZID=America/New_York"
        assert value == "20260714T100000"

    def test_offset_without_timezone_converts_to_utc(self):
        assert _format_dt_value({"dateTime": "2026-07-14T10:00:00-04:00"}) == (
            "", "20260714T140000Z",
        )

    def test_empty_dict_returns_none(self):
        assert _format_dt_value({}) is None


# ---------------------------------------------------------------------------
# EXDATE emission (via _events_to_ics)
# ---------------------------------------------------------------------------


class TestExdateEmission:
    def test_timed_cancelled_instance_emits_exdate_with_tzid_param(self):
        parent = _timed_event(
            id="parent1",
            start={"dateTime": "2026-07-07T10:00:00-04:00", "timeZone": "America/New_York"},
            end={"dateTime": "2026-07-07T11:00:00-04:00", "timeZone": "America/New_York"},
            recurrence=["RRULE:FREQ=WEEKLY"],
        )
        cancelled = {
            "id": "parent1_20260714T140000Z",
            "status": "cancelled",
            "recurringEventId": "parent1",
            "originalStartTime": {
                "dateTime": "2026-07-14T10:00:00-04:00",
                "timeZone": "America/New_York",
            },
        }
        ics = _events_to_ics([parent, cancelled], "Test Cal")
        assert "EXDATE;TZID=America/New_York:20260714T100000" in ics
        # The old bug: parameter after the colon
        assert "EXDATE:TZID=" not in ics

    def test_all_day_cancelled_instance_emits_value_date_param(self):
        # EXDATE defaults to DATE-TIME, so an all-day exclusion must
        # carry ;VALUE=DATE to match the DTSTART;VALUE=DATE parent.
        parent = {
            "id": "parent2",
            "summary": "Daily standup",
            "status": "confirmed",
            "start": {"date": "2026-07-07"},
            "end": {"date": "2026-07-08"},
            "recurrence": ["RRULE:FREQ=DAILY"],
        }
        cancelled = {
            "id": "parent2_20260714",
            "status": "cancelled",
            "recurringEventId": "parent2",
            "originalStartTime": {"date": "2026-07-14"},
        }
        ics = _events_to_ics([parent, cancelled], "Test Cal")
        assert "EXDATE;VALUE=DATE:20260714" in ics

    def test_utc_cancelled_instance_emits_plain_exdate(self):
        parent = _timed_event(id="parent3", recurrence=["RRULE:FREQ=WEEKLY"])
        cancelled = {
            "id": "parent3_20260714T100000Z",
            "status": "cancelled",
            "recurringEventId": "parent3",
            "originalStartTime": {"dateTime": "2026-07-14T10:00:00Z"},
        }
        ics = _events_to_ics([parent, cancelled], "Test Cal")
        assert "EXDATE:20260714T100000Z" in ics


# ---------------------------------------------------------------------------
# _clean_timestamp
# ---------------------------------------------------------------------------


class TestCleanTimestamp:
    def test_strips_literal_000_millis(self):
        assert _clean_timestamp("2026-07-07T10:00:00.000Z") == "20260707T100000Z"

    def test_strips_real_fractional_seconds(self):
        assert _clean_timestamp("2026-07-07T10:00:00.123Z") == "20260707T100000Z"

    def test_no_fraction_unchanged(self):
        assert _clean_timestamp("2026-07-07T10:00:00Z") == "20260707T100000Z"


# ---------------------------------------------------------------------------
# Parameter quoting (CN=) and text escaping
# ---------------------------------------------------------------------------


class TestQuoteParam:
    def test_plain_name_unquoted(self):
        assert _quote_param("John Doe") == "John Doe"

    def test_comma_forces_quoting(self):
        assert _quote_param("Doe, John") == '"Doe, John"'

    def test_semicolon_and_colon_force_quoting(self):
        assert _quote_param("a;b") == '"a;b"'
        assert _quote_param("a:b") == '"a:b"'

    def test_dquote_replaced(self):
        # DQUOTE can never appear inside a parameter value
        assert _quote_param('John "JD" Doe') == "John 'JD' Doe"

    def test_newlines_collapsed(self):
        assert _quote_param("John\r\nDoe") == "John  Doe"

    def test_attendee_cn_is_quoted_not_backslash_escaped(self):
        event = _timed_event(attendees=[
            {"email": "jd@example.com", "displayName": "Doe, John",
             "responseStatus": "accepted"},
        ])
        vevent = _event_to_vevent(event)
        assert 'CN="Doe, John"' in vevent
        assert "CN=Doe\\," not in vevent

    def test_organizer_cn_is_quoted(self):
        event = _timed_event(
            organizer={"email": "org@example.com", "displayName": "Smith, Jane"},
        )
        vevent = _event_to_vevent(event)
        assert 'ORGANIZER;CN="Smith, Jane":mailto:org@example.com' in vevent


class TestEscapeIcs:
    def test_comma_and_semicolon_escaped(self):
        assert _escape_ics("a,b;c") == "a\\,b\\;c"

    def test_backslash_escaped_first(self):
        assert _escape_ics("a\\b") == "a\\\\b"

    def test_newline_escaped(self):
        assert _escape_ics("line1\nline2") == "line1\\nline2"

    def test_crlf_collapses_to_single_escaped_newline(self):
        assert _escape_ics("line1\r\nline2") == "line1\\nline2"

    def test_lone_cr_does_not_survive_raw(self):
        result = _escape_ics("line1\rline2")
        assert "\r" not in result
        assert result == "line1\\nline2"


# ---------------------------------------------------------------------------
# _unique_entry_name
# ---------------------------------------------------------------------------


class TestUniqueEntryName:
    def test_first_use_is_plain(self):
        used: set[str] = set()
        assert _unique_entry_name("cal", used) == "cal.ics"

    def test_duplicates_get_numeric_suffix(self):
        used: set[str] = set()
        assert _unique_entry_name("cal", used) == "cal.ics"
        assert _unique_entry_name("cal", used) == "cal-2.ics"
        assert _unique_entry_name("cal", used) == "cal-3.ics"

    def test_used_set_is_recorded(self):
        used: set[str] = set()
        _unique_entry_name("cal", used)
        assert used == {"cal.ics"}


# ---------------------------------------------------------------------------
# _is_busybridge_event — shared predicate agreement
# ---------------------------------------------------------------------------


def _managed_id() -> str:
    from app.ledger.identity import derive_google_event_id

    return derive_google_event_id(123)


class TestBusyBridgePredicateAgreement:
    """The ICS clean export and the backup snapshot must classify
    events identically — both delegate to
    app.ledger.identity.is_busybridge_event."""

    @pytest.mark.parametrize(
        "event, expected",
        [
            # Deterministic managed ledger ID
            (lambda s: {"id": _managed_id()}, True),
            # bb_proj_id defence-in-depth stamp
            (lambda s: {
                "id": "randomid123",
                "extendedProperties": {"private": {"bb_proj_id": "42"}},
            }, True),
            # Legacy sync-tag extended property
            (lambda s: {
                "id": "randomid456",
                "extendedProperties": {"private": {s.calendar_sync_tag: "true"}},
            }, True),
            # Legacy prefix-titled event (no extended properties at all)
            (lambda s: {
                "id": "randomid789",
                "summary": f"{s.managed_event_prefix} Busy",
            }, True),
            # Unmanaged event
            (lambda s: {
                "id": "randomid000",
                "summary": "Dentist appointment",
                "extendedProperties": {"private": {"someone_elses_tag": "true"}},
            }, False),
        ],
        ids=["managed-id", "bb_proj_id", "sync-tag", "prefix-title", "unmanaged"],
    )
    def test_ics_export_and_backup_snapshot_agree(self, event, expected):
        from app.config import get_settings
        from app.sync.google_calendar import GoogleCalendarClient
        from app.sync.ics_export import _is_busybridge_event

        settings = get_settings()
        event = event(settings)

        # Backup snapshot path: client method
        client = object.__new__(GoogleCalendarClient)
        client.settings = settings
        via_backup = client.is_our_event(event)

        # ICS clean-export path: module helper
        via_ics = _is_busybridge_event(event)

        assert via_backup == via_ics == expected


# ---------------------------------------------------------------------------
# create_ics_backup
# ---------------------------------------------------------------------------


class TestCreateIcsBackup:
    async def test_duplicate_calendar_names_get_distinct_entries_and_errors_propagate(
        self, test_db, tmp_path, monkeypatch
    ):
        from app.sync.ics_export import create_ics_backup

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        uid1 = await _insert_user("ics-a@example.com", "ics-google-a")
        uid2 = await _insert_user("ics-b@example.com", "ics-google-b")

        # Both users report a calendar with the SAME display name; the
        # second user also reports a per-calendar fetch failure.
        async def fake_fetch(user_id: int):
            calendars = [{
                "calendar_name": "Main - shared name",
                "events": [_timed_event(id=f"evt-{user_id}")],
            }]
            errors = []
            if user_id == uid2:
                errors.append(f"user {user_id}: client calendar 9 failed: boom")
            return calendars, errors

        monkeypatch.setattr(
            "app.sync.ics_export._fetch_all_user_calendars", fake_fetch
        )

        metadata = await create_ics_backup()

        assert metadata["total_calendars"] == 2
        # Per-calendar fetch failures surface in metadata instead of
        # being swallowed as an empty errors list.
        assert metadata["errors"] == [f"user {uid2}: client calendar 9 failed: boom"]

        full_path = tmp_path / "ics" / f"{metadata['full_backup_id']}.zip"
        with zipfile.ZipFile(str(full_path)) as zf:
            names = zf.namelist()
            # Two calendars → two distinct entries (no silent clobber)
            assert len(names) == 2
            assert len(set(names)) == 2

    async def test_backup_ids_carry_collision_suffix(
        self, test_db, tmp_path, monkeypatch
    ):
        import re as _re
        from app.sync.ics_export import create_ics_backup

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))

        metadata = await create_ics_backup()  # no users → empty backup
        assert _re.fullmatch(
            r"ics-full-\d{8}-\d{6}-[0-9a-f]{6}", metadata["full_backup_id"]
        )
        assert _re.fullmatch(
            r"ics-clean-\d{8}-\d{6}-[0-9a-f]{6}", metadata["clean_backup_id"]
        )

    async def test_failure_mid_export_leaves_no_partial_zips(
        self, test_db, tmp_path, monkeypatch
    ):
        from app.sync.ics_export import create_ics_backup

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        await _insert_user("ics-fail@example.com", "ics-google-fail")

        async def fake_fetch(user_id: int):
            return [{"calendar_name": "Main", "events": [_timed_event()]}], []

        def boom(*args, **kwargs):
            raise RuntimeError("disk exploded")

        monkeypatch.setattr(
            "app.sync.ics_export._fetch_all_user_calendars", fake_fetch
        )
        monkeypatch.setattr("app.sync.ics_export._events_to_ics", boom)

        with pytest.raises(RuntimeError, match="disk exploded"):
            await create_ics_backup()

        # No valid-looking partial ZIPs left for retention to count
        ics_dir = tmp_path / "ics"
        assert [f for f in os.listdir(ics_dir) if f.endswith(".zip")] == []

    async def test_listing_and_retention_parse_suffixed_timestamps(
        self, test_db, tmp_path, monkeypatch
    ):
        from app.sync.ics_export import create_ics_backup, list_ics_backups

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        metadata = await create_ics_backup()

        backups = list_ics_backups()
        assert len(backups) == 1
        # created_at parses despite the random suffix in the timestamp
        assert backups[0]["created_at"].startswith(metadata["created_at"][:10])
        assert backups[0]["full_backup_id"] == metadata["full_backup_id"]
        assert backups[0]["clean_backup_id"] == metadata["clean_backup_id"]
