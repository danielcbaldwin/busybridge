"""A read-only (webcal/personal) live modified occurrence deleted on main must
be re-asserted, never silently cancelled.

Regression: the churn-breaker's drift-revert was gated on
``source_type == 'client'``, so a still-live modified WEBCAL occurrence whose
main copy was deleted fell through to ``_mark_managed_instance_cancelled`` —
permanently cancelling a meeting that is still live in the feed and dropping
its busy block on every target (a silent double-booking exposure).  Webcal and
personal are read-only sources: deleting the main copy is pure drift to revert.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


def _ics(*vevents):
    body = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//Test//EN"]
    for v in vevents:
        body.append("BEGIN:VEVENT")
        body.extend(line for line in v.strip().splitlines())
        body.append("END:VEVENT")
    body.append("END:VCALENDAR")
    return ("\r\n".join(body) + "\r\n").encode("utf-8")


# Weekly webcal series with a MODIFIED (live) occurrence moved on 2026-02-16.
WEBCAL_BODY = _ics(
    "UID:weekly@example.com\nSUMMARY:WeeklyWebcal\nDTSTART:20260202T090000Z\n"
    "DTEND:20260202T093000Z\nRRULE:FREQ=WEEKLY;COUNT=8",
    "UID:weekly@example.com\nSUMMARY:WeeklyWebcal\nDTSTART:20260216T140000Z\n"
    "DTEND:20260216T143000Z\nRECURRENCE-ID:20260216T090000Z",
)


async def _fetch(url, if_none_match):
    return {"status": 200, "etag": '"v1"', "body": WEBCAL_BODY}


async def _quiesce(s, max_passes=10):
    for _ in range(max_passes):
        await s.run_reconciler("alice", webcal_fetch=_fetch)
        row = await (await s._db.execute(
            "SELECT COUNT(*) AS n FROM outbox_operations WHERE status='pending'"
        )).fetchone()
        if int(row["n"]) == 0:
            break
        s.advance(timedelta(seconds=1))


async def test_live_webcal_modified_instance_deleted_on_main_is_reverted():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    await s.given_webcal("alice", sub_nick="feed", url="https://e.test/f.ics")
    await _quiesce(s)

    prow = await (await s._db.execute(
        "SELECT p.google_event_id FROM ledger_projections p "
        "JOIN ledger_events e ON e.id=p.ledger_event_id "
        "WHERE e.user_id=? AND e.source_type='webcal' "
        "AND e.parent_canonical_uid IS NULL AND p.target_kind='main' LIMIT 1",
        (user.user_id,),
    )).fetchone()
    gid = prow["google_event_id"]

    inst = await (await s._db.execute(
        "SELECT id, status FROM ledger_events "
        "WHERE user_id=? AND parent_canonical_uid IS NOT NULL",
        (user.user_id,),
    )).fetchone()
    assert inst is not None and inst["status"] == "active"

    mods = [
        e for e in s.list_events("main", single_events=True)
        if e.get("recurringEventId") == gid
        and e.get("start", {}).get("dateTime") == "2026-02-16T14:00:00Z"
    ]
    assert mods, "modified main copy not found"

    def peer_busy_0216():
        return [
            e for e in s.list_events("client_a", single_events=True)
            if e.get("status") != "cancelled"
            and e.get("start", {}).get("dateTime", "").startswith("2026-02-16")
        ]

    assert len(peer_busy_0216()) == 1

    # User deletes the modified main copy of a STILL-LIVE webcal occurrence.
    s.google.delete_event(s.cal("main"), mods[0]["id"])
    await _quiesce(s)
    await _quiesce(s)

    inst_after = await (await s._db.execute(
        "SELECT status FROM ledger_events "
        "WHERE user_id=? AND parent_canonical_uid IS NOT NULL",
        (user.user_id,),
    )).fetchone()
    assert inst_after["status"] == "active", (
        "live webcal modified occurrence was wrongly cancelled after its main "
        "copy was deleted (should revert as read-only drift)"
    )
    assert len(peer_busy_0216()) == 1, "peer busy block for a live occurrence was dropped"
    await s.close()
