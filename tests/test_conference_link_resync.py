"""Conference-link (Google Meet) change detection — churn-safe.

A source Meet link that gets regenerated must re-sync to the mirrored
main copy, but the detection must NOT revive the version-905 flip-flop
churn that made conferenceData excluded from the content hash in the
first place (Google returns a modified recurring instance's own link on
one API surface and the inherited master link on another, alternating
across reads).

The fix (``client.py:_resolve_conference``) keeps conferenceData out of
the content hash but accepts a new ``conferenceId`` only after it is seen
on TWO consecutive ingests — so an alternating read never confirms, while
a genuine, settled change confirms on the next read and propagates.
"""

from __future__ import annotations

import json

import pytest

from app.ledger.ingest.client import _conference_id, _resolve_conference
from tests.integration.framework import Scenario


def _conf(cid: str) -> str:
    """A conferenceData blob (as stored JSON) for room ``cid``."""
    return json.dumps({
        "conferenceId": cid,
        "entryPoints": [
            {"entryPointType": "video", "uri": f"https://meet.google.com/{cid}"},
        ],
    })


def _existing(conf_json=None, pending=None) -> dict:
    return {"conference_data_json": conf_json, "pending_conference_id": pending}


# ---------------------------------------------------------------------------
# Unit: the debounce state machine
# ---------------------------------------------------------------------------
def test_same_room_is_no_change_and_clears_candidate():
    stored, pending, changed = _resolve_conference(
        _existing(_conf("aaa"), pending="bbb"),
        {"conference_data_json": _conf("aaa")},
    )
    assert changed is False
    assert pending is None              # stale candidate dropped
    assert _conference_id(stored) == "aaa"


def test_first_sighting_remembers_candidate_but_holds_value():
    stored, pending, changed = _resolve_conference(
        _existing(_conf("aaa"), pending=None),
        {"conference_data_json": _conf("bbb")},
    )
    assert changed is False             # not yet — one read is not enough
    assert pending == "bbb"             # candidate remembered
    assert _conference_id(stored) == "aaa"   # accepted value held


def test_confirms_on_second_consecutive_read():
    stored, pending, changed = _resolve_conference(
        _existing(_conf("aaa"), pending="bbb"),   # bbb already seen once
        {"conference_data_json": _conf("bbb")},
    )
    assert changed is True
    assert pending is None
    assert _conference_id(stored) == "bbb"


def test_flip_flop_never_confirms():
    """The exact version-905 pathology: alternating reads of the master
    link (uym) and a modified instance's own link (vjs) must never flip
    the stored value, so no re-plan / re-send churn is generated."""
    stored_json = _conf("uym")
    pending = None
    for i in range(8):
        incoming = _conf("vjs") if i % 2 == 0 else _conf("uym")
        stored_json, pending, changed = _resolve_conference(
            _existing(stored_json, pending),
            {"conference_data_json": incoming},
        )
        assert changed is False, f"flip-flop confirmed on read {i}"
        assert _conference_id(stored_json) == "uym"


def test_entry_point_noise_within_same_room_is_ignored():
    a = json.dumps({"conferenceId": "aaa",
                    "entryPoints": [{"uri": "x"}, {"uri": "y"}]})
    b = json.dumps({"conferenceId": "aaa",
                    "entryPoints": [{"uri": "y"}, {"uri": "x"}, {"uri": "z"}]})
    stored, pending, changed = _resolve_conference(
        _existing(a, pending=None), {"conference_data_json": b},
    )
    assert changed is False
    assert pending is None


def test_gaining_a_conference_is_debounced_then_accepted():
    e = _existing(None, pending=None)
    stored, pending, changed = _resolve_conference(
        e, {"conference_data_json": _conf("aaa")})
    assert changed is False and pending == "aaa" and stored is None
    stored, pending, changed = _resolve_conference(
        _existing(None, pending="aaa"), {"conference_data_json": _conf("aaa")})
    assert changed is True and _conference_id(stored) == "aaa"


# ---------------------------------------------------------------------------
# Integration: a settled source link change re-syncs to the main copy
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_changed_source_meet_link_resyncs_to_main_copy():
    old = {"conferenceId": "old-room",
           "entryPoints": [{"entryPointType": "video",
                            "uri": "https://meet.google.com/old-room"}]}
    new = {"conferenceId": "new-room",
           "entryPoints": [{"entryPointType": "video",
                            "uri": "https://meet.google.com/new-room"}]}
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])
    ev = s.given_event(
        "client_a", summary="Weekly sync", start="2026-03-03T09:00:00Z",
        conference_data=old,
        attendees=[{"email": "alice@example.com", "self": True,
                    "responseStatus": "accepted"}],
    )
    await s.run_reconciler("alice")
    copy = s.assert_event_exists("main", summary_contains="Weekly sync")
    assert copy["conferenceData"]["conferenceId"] == "old-room"

    # The organizer regenerates the Meet link on the source (conference
    # only — no other field changes).
    s.update_event("client_a", ev["id"], conferenceData=new)

    # First audit read: a single sighting must NOT yet flip the link.
    await s.run_audit("alice")
    copy = s.assert_event_exists("main", summary_contains="Weekly sync")
    assert copy["conferenceData"]["conferenceId"] == "old-room", (
        "a single read of the new link should be debounced, not applied"
    )

    # Second (confirming) read: now it propagates to the main copy.
    await s.run_audit("alice")
    copy = s.assert_event_exists("main", summary_contains="Weekly sync")
    assert copy["conferenceData"]["conferenceId"] == "new-room", (
        "a settled source Meet-link change must re-sync to the main copy"
    )

    # Converged: a further audit is a no-op (no re-send churn).
    out = await s.run_audit("alice")
    assert out.get("drain", {}).get("succeeded", 0) == 0, (
        f"conference re-sync churned after converging: {out}"
    )
    await s.close()
