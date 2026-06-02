"""Cross-source de-duplication of recurring busy blocks.

A meeting the user attends often lands on BOTH a client/personal calendar
AND natively on their main calendar, producing a parallel 'main_native'
ledger lineage for the same meeting. Both lineages then cast busy blocks
onto the other client calendars — a visible duplicate. The fix suppresses
the redundant main_native lineage, matched by Google's cross-calendar
iCalUID.

Critically, matching is by iCalUID, NOT by time: two genuinely different
meetings at the same start time must each keep their busy block (merging
them would hide a real conflict — the worst outcome for this tool).
"""

from __future__ import annotations

import pytest

from app.database import get_database
from app.ledger.planner import plan_for_ledger_event

pytestmark = pytest.mark.asyncio


async def _user_with_peer_client(db):
    """User with two active client calendars: an 'origin' (where the client
    event natively lives) and a separate 'peer' (which receives busy
    blocks).  Two are needed so a client event has a peer to cast a
    present_busy block onto."""
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id) VALUES ('u@x.com', 'g1')")
    uid = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO oauth_tokens (user_id, account_type, google_account_email, "
        " access_token_encrypted, refresh_token_encrypted) "
        "VALUES (?, 'client', 'c@x.com', ?, ?)", (uid, b"a", b"b"))
    tok = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO client_calendars (user_id, oauth_token_id, "
        " google_calendar_id, display_name, is_active) "
        "VALUES (?, ?, 'origin@cal', 'Origin', 1)", (uid, tok))
    origin = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO client_calendars (user_id, oauth_token_id, "
        " google_calendar_id, display_name, is_active) "
        "VALUES (?, ?, 'peer@cal', 'Peer', 1)", (uid, tok))
    peer = int(cur.lastrowid)
    await db.commit()
    return uid, origin, peer


async def _ledger_event(db, uid, *, source_type, ical_uid, canonical,
                        start="2026-07-10T17:00:00Z", source_cal=None):
    cur = await db.execute(
        "INSERT INTO ledger_events "
        "(user_id, canonical_uid, source_type, source_calendar_id, ical_uid, "
        " summary, start_at, end_at, show_as, status, version) "
        "VALUES (?, ?, ?, ?, ?, 'Mtg', ?, '2026-07-10T18:00:00Z', 'busy', "
        " 'active', 1)",
        (uid, canonical, source_type, source_cal, ical_uid, start))
    await db.commit()
    return int(cur.lastrowid)


async def _peer_states(db, ledger_event_id):
    return [
        (r["target_kind"], r["target_calendar_id"], r["desired_state"])
        for r in await (await db.execute(
            "SELECT target_kind, target_calendar_id, desired_state "
            "FROM ledger_projections WHERE ledger_event_id=?",
            (ledger_event_id,))).fetchall()
    ]


async def test_redundant_main_native_is_suppressed(test_db):
    db = await get_database()
    uid, origin, peer = await _user_with_peer_client(db)
    # Same meeting (iCalUID 'U') ingested from a client source AND natively.
    client_le = await _ledger_event(
        db, uid, source_type="client", ical_uid="U", canonical="client:1:abc",
        source_cal=origin)
    native_le = await _ledger_event(
        db, uid, source_type="main_native", ical_uid="U",
        canonical="main_native:1:xyz")

    await plan_for_ledger_event(db, ledger_event_id=client_le)
    await plan_for_ledger_event(db, ledger_event_id=native_le)

    # The client source still casts a busy block on the peer calendar.
    client_states = await _peer_states(db, client_le)
    assert any(s == "present_busy" for (_, _, s) in client_states), client_states

    # The main_native reflection projects NOTHING (all absent) — no dup.
    native_states = await _peer_states(db, native_le)
    assert native_states, "expected projection rows for the native event"
    assert all(s == "absent" for (_, _, s) in native_states), native_states


async def test_genuine_native_main_event_still_projects(test_db):
    # No client/personal sibling → a real native main event must still
    # cast busy blocks (the dedup must not over-suppress).
    db = await get_database()
    uid, origin, peer = await _user_with_peer_client(db)
    native_le = await _ledger_event(
        db, uid, source_type="main_native", ical_uid="ONLY",
        canonical="main_native:1:solo")

    await plan_for_ledger_event(db, ledger_event_id=native_le)

    states = await _peer_states(db, native_le)
    assert any(s == "present_busy" for (_, _, s) in states), states


async def test_distinct_meetings_same_time_are_not_merged(test_db):
    # The safety property: a native event whose iCalUID does NOT match the
    # client event — even at the SAME start time — must NOT be suppressed.
    # Merging them would drop a real busy block (show free when busy).
    db = await get_database()
    uid, origin, peer = await _user_with_peer_client(db)
    same_time = "2026-07-10T17:00:00Z"
    await _ledger_event(
        db, uid, source_type="client", ical_uid="AAA",
        canonical="client:1:aaa", start=same_time, source_cal=origin)
    native_le = await _ledger_event(
        db, uid, source_type="main_native", ical_uid="BBB",  # different mtg
        canonical="main_native:1:bbb", start=same_time)

    await plan_for_ledger_event(db, ledger_event_id=native_le)

    states = await _peer_states(db, native_le)
    assert any(s == "present_busy" for (_, _, s) in states), (
        "a distinct same-time meeting must keep its busy block, not be "
        f"merged away: {states}")


class _OnePageGoogle:
    def __init__(self, items):
        self._items = items

    def list_events(self, cal, *, sync_token=None, page_token=None,
                    show_deleted=False, max_results=250):
        return {"items": self._items, "nextSyncToken": "TOK"}


async def test_ingest_captures_icaluid(test_db):
    # The dedup depends on ical_uid being captured on ingest; verify the
    # real client ingest path stores it.
    from app.ledger.ingest.client import ingest_client_calendar
    db = await get_database()
    uid, origin, peer = await _user_with_peer_client(db)
    google = _OnePageGoogle([{
        "id": "evt00abc", "status": "confirmed", "summary": "M",
        "iCalUID": "the-uid@google.com",
        "start": {"dateTime": "2026-07-10T10:00:00Z"},
        "end": {"dateTime": "2026-07-10T11:00:00Z"},
    }])
    await ingest_client_calendar(
        db, google, user_id=uid, client_calendar_id=origin,
        google_calendar_id="origin@cal", user_email="u@x.com")
    row = await (await db.execute(
        "SELECT ical_uid FROM ledger_events "
        "WHERE source_calendar_id=? AND source_type='client'", (origin,)
    )).fetchone()
    assert row is not None and row["ical_uid"] == "the-uid@google.com"


async def test_main_native_without_icaluid_is_not_suppressed(test_db):
    # A main_native row with no iCalUID (older row, or an event Google
    # didn't give one) can't be matched, so it must project normally
    # rather than being silently dropped.
    db = await get_database()
    uid, origin, peer = await _user_with_peer_client(db)
    await _ledger_event(
        db, uid, source_type="client", ical_uid="U", canonical="client:1:u",
        source_cal=origin)
    native_le = await _ledger_event(
        db, uid, source_type="main_native", ical_uid=None,
        canonical="main_native:1:noical")

    await plan_for_ledger_event(db, ledger_event_id=native_le)

    states = await _peer_states(db, native_le)
    assert any(s == "present_busy" for (_, _, s) in states), states
