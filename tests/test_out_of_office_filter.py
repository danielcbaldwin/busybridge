"""Google ``eventType='outOfOffice'`` events must not fan out as busy
blocks on peer calendars by default.

Rationale: OOO on ONE calendar is a status signal — the user is out
from THAT organization.  A user OOO from Work A may still be actively
working out of Work B (or their personal calendar), so casting the
OOO as a busy block on those other calendars misrepresents their
actual availability.  The ``SYNC_OUT_OF_OFFICE_EVENTS`` setting
opts back into the legacy behaviour.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.config import Settings


@pytest.mark.asyncio
async def test_ooo_event_is_skipped_on_client_ingest_by_default(monkeypatch):
    """Client-source OOO events return 'skipped' from _ingest_one_event
    without touching the ledger."""
    cfg = Settings(sync_out_of_office_events=False)
    monkeypatch.setattr("app.config.get_settings", lambda: cfg)

    from app.ledger.ingest.client import _ingest_one_event

    ooo_event = {
        "id": "some-google-event-id",
        "status": "confirmed",
        "eventType": "outOfOffice",
        "start": {"date": "2026-07-16"},
        "end": {"date": "2026-07-17"},
        "summary": "OOO",
    }
    db = AsyncMock()
    outcome, ledger_id = await _ingest_one_event(
        db, user_id=1, client_calendar_id=1, user_email="u@x", event=ooo_event,
    )
    assert outcome == "skipped"
    assert ledger_id is None
    # Zero DB reads/writes when OOO is filtered upfront.
    db.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_ooo_event_is_skipped_on_personal_ingest_by_default(monkeypatch):
    """Personal-source OOO events return 'skipped' from _ingest_one
    without touching the ledger."""
    cfg = Settings(sync_out_of_office_events=False)
    monkeypatch.setattr("app.config.get_settings", lambda: cfg)

    from app.ledger.ingest.personal import _ingest_one

    ooo_event = {
        "id": "some-google-event-id",
        "status": "confirmed",
        "eventType": "outOfOffice",
        "start": {"date": "2026-07-16"},
        "end": {"date": "2026-07-17"},
        "summary": "OOO",
    }
    db = AsyncMock()
    outcome, ledger_id = await _ingest_one(
        db, user_id=1, personal_calendar_id=1, user_email="u@x", event=ooo_event,
    )
    assert outcome == "skipped"
    assert ledger_id is None
    db.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_ooo_event_is_ingested_when_flag_is_on(monkeypatch):
    """Flipping ``SYNC_OUT_OF_OFFICE_EVENTS=True`` restores the legacy
    behavior — the OOO check falls through and normal ingest logic
    runs (verified by asserting the DB is touched)."""
    cfg = Settings(sync_out_of_office_events=True)
    monkeypatch.setattr("app.config.get_settings", lambda: cfg)

    from app.ledger.ingest.personal import _ingest_one

    ooo_event = {
        "id": "some-google-event-id",
        "status": "confirmed",
        "eventType": "outOfOffice",
        "start": {"date": "2026-07-16"},
        "end": {"date": "2026-07-17"},
        "summary": "OOO",
    }
    db = AsyncMock()
    # The subsequent projection-match SELECT will hit the mock; we just
    # need to prove ingest DID call it (i.e. did NOT short-circuit).
    db.execute.return_value.__aenter__ = AsyncMock()
    try:
        await _ingest_one(
            db, user_id=1, personal_calendar_id=1, user_email="u@x", event=ooo_event,
        )
    except Exception:
        pass  # normal ingest will error on the mock; we only care that it started
    db.execute.assert_awaited()


@pytest.mark.asyncio
async def test_non_ooo_event_ingest_unchanged(monkeypatch):
    """Sanity: a regular (non-OOO) event does NOT hit the OOO filter
    and proceeds into the normal ingest path (touches the DB)."""
    cfg = Settings(sync_out_of_office_events=False)
    monkeypatch.setattr("app.config.get_settings", lambda: cfg)

    from app.ledger.ingest.personal import _ingest_one

    normal_event = {
        "id": "some-google-event-id",
        "status": "confirmed",
        # No eventType key → treated as default; ingest proceeds.
        "start": {"dateTime": "2026-07-16T09:00:00Z"},
        "end": {"dateTime": "2026-07-16T10:00:00Z"},
        "summary": "Meeting",
    }
    db = AsyncMock()
    try:
        await _ingest_one(
            db, user_id=1, personal_calendar_id=1, user_email="u@x", event=normal_event,
        )
    except Exception:
        pass
    db.execute.assert_awaited()


# ---------------------------------------------------------------------------
# Description-tag opt-out
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_description_tag_skips_event(monkeypatch):
    """An event whose description contains the configured skip tag is
    skipped at ingest without touching the ledger."""
    cfg = Settings(skip_event_description_tag="[nosync]")
    monkeypatch.setattr("app.config.get_settings", lambda: cfg)

    from app.ledger.ingest.personal import _ingest_one

    ev = {
        "id": "e-1",
        "status": "confirmed",
        "start": {"dateTime": "2026-07-16T09:00:00Z"},
        "end": {"dateTime": "2026-07-16T10:00:00Z"},
        "summary": "Prep block",
        "description": "personal prep [nosync]",
    }
    db = AsyncMock()
    outcome, ledger_id = await _ingest_one(
        db, user_id=1, personal_calendar_id=1, user_email="u@x", event=ev,
    )
    assert outcome == "skipped"
    assert ledger_id is None
    db.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_description_tag_is_case_insensitive(monkeypatch):
    """`[NoSync]` in description matches configured `[nosync]`."""
    cfg = Settings(skip_event_description_tag="[nosync]")
    monkeypatch.setattr("app.config.get_settings", lambda: cfg)

    from app.ledger.ingest.personal import _ingest_one

    ev = {
        "id": "e-1",
        "status": "confirmed",
        "start": {"dateTime": "2026-07-16T09:00:00Z"},
        "end": {"dateTime": "2026-07-16T10:00:00Z"},
        "description": "prep block [NoSync]",
    }
    db = AsyncMock()
    outcome, _ = await _ingest_one(
        db, user_id=1, personal_calendar_id=1, user_email="u@x", event=ev,
    )
    assert outcome == "skipped"


@pytest.mark.asyncio
async def test_description_tag_empty_disables_check(monkeypatch):
    """Setting the tag to empty disables the check; a description
    containing '[nosync]' is ingested normally."""
    cfg = Settings(skip_event_description_tag="")
    monkeypatch.setattr("app.config.get_settings", lambda: cfg)

    from app.ledger.ingest.personal import _ingest_one

    ev = {
        "id": "e-1",
        "status": "confirmed",
        "start": {"dateTime": "2026-07-16T09:00:00Z"},
        "end": {"dateTime": "2026-07-16T10:00:00Z"},
        "description": "prep block [nosync]",
    }
    db = AsyncMock()
    try:
        await _ingest_one(
            db, user_id=1, personal_calendar_id=1, user_email="u@x", event=ev,
        )
    except Exception:
        pass  # normal ingest paths hit the mock; we only care it started
    db.execute.assert_awaited()
