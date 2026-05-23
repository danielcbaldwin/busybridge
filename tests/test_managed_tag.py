"""The [BusyBridge] description tag.

Every event BusyBridge writes carries a marker in its DESCRIPTION
(default ``[BusyBridge]``, from MANAGED_EVENT_PREFIX) so a user can
find — and worst-case bulk-delete — all of them by searching their
calendar.  It lives in the description, not the title, so events read
normally at a glance.

Invariants pinned here:
* busy blocks and full main-copies are tagged;
* the origin write-back to the user's REAL source event is NEVER
  tagged, and an edit made on a tagged copy is written back WITHOUT
  the tag (no leak, no double-tagging);
* an empty prefix disables tagging entirely.
"""

from __future__ import annotations

import types

import pytest

from app.ledger import payload
from app.ledger.payload import (
    PRESENT_BUSY,
    PRESENT_FULL,
    PRESENT_FULL_RSVP_ONLY,
    PRESENT_PERSONAL_BUSY,
    render_payload,
    strip_managed_tag,
)
from tests.integration.framework import Scenario

TAG = "[BusyBridge]"


def _row(**over) -> dict:
    base = {
        "summary": "Real Title",
        "description": None,
        "location": None,
        "start_at": "2026-02-02T09:00:00Z",
        "end_at": "2026-02-02T09:30:00Z",
        "start_timezone": None,
        "end_timezone": None,
        "is_all_day": False,
        "show_as": "busy",
        "color_id": None,
        "user_can_edit": True,
        "user_rsvp_status": None,
        "recurrence_rule_json": None,
        "attendees_json": None,
        "source_type": "client",
    }
    base.update(over)
    return base


# ---------------------------------------------------------------------------
# Renderers carry the tag
# ---------------------------------------------------------------------------
def test_busy_block_description_is_the_tag():
    body = render_payload(desired_state=PRESENT_BUSY, ledger_row=_row())
    assert body["description"] == TAG


def test_personal_busy_description_is_the_tag():
    body = render_payload(desired_state=PRESENT_PERSONAL_BUSY, ledger_row=_row())
    assert body["description"] == TAG


def test_full_copy_appends_tag_to_real_description():
    body = render_payload(
        desired_state=PRESENT_FULL,
        ledger_row=_row(description="Agenda: ship it"),
        target_kind="main",
    )
    assert body["description"] == f"Agenda: ship it\n\n{TAG}"


def test_full_copy_with_no_description_is_just_the_tag():
    body = render_payload(
        desired_state=PRESENT_FULL, ledger_row=_row(description=None),
        target_kind="main",
    )
    assert body["description"] == TAG


def test_origin_writeback_is_never_tagged():
    # The write-back patches the user's REAL source event — it must
    # carry the clean description, never our tag.
    body = render_payload(
        desired_state=PRESENT_FULL_RSVP_ONLY,
        ledger_row=_row(description="Agenda: ship it", user_rsvp_status="accepted"),
    )
    assert body["description"] == "Agenda: ship it"
    assert TAG not in (body.get("description") or "")


# ---------------------------------------------------------------------------
# strip is the inverse
# ---------------------------------------------------------------------------
def test_strip_round_trips_real_description():
    tagged = render_payload(
        desired_state=PRESENT_FULL, ledger_row=_row(description="Hello"),
        target_kind="main",
    )["description"]
    assert strip_managed_tag(tagged) == "Hello"


def test_strip_of_tag_only_is_none():
    assert strip_managed_tag(TAG) is None


def test_strip_is_noop_when_no_tag_present():
    assert strip_managed_tag("just a normal note") == "just a normal note"
    assert strip_managed_tag(None) is None


# ---------------------------------------------------------------------------
# Empty prefix disables tagging
# ---------------------------------------------------------------------------
def test_empty_prefix_disables_tagging(monkeypatch):
    monkeypatch.setattr(
        payload, "get_settings",
        lambda: types.SimpleNamespace(managed_event_prefix=""),
    )
    busy = render_payload(desired_state=PRESENT_BUSY, ledger_row=_row())
    assert "description" not in busy
    full = render_payload(
        desired_state=PRESENT_FULL, ledger_row=_row(description="Hi"),
        target_kind="main",
    )
    assert full["description"] == "Hi"
    assert strip_managed_tag("Hi") == "Hi"


# ---------------------------------------------------------------------------
# End-to-end: tag appears on our writes, never on the source
# ---------------------------------------------------------------------------
def _desc(scenario: Scenario, calendar: str, summary_contains: str) -> str:
    for ev in scenario.list_events(calendar):
        if summary_contains in (ev.get("summary") or ""):
            return ev.get("description") or ""
    raise AssertionError(f"no event matching {summary_contains!r} on {calendar!r}")


@pytest.mark.asyncio
async def test_mirrored_writes_are_tagged_but_source_is_not():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    await s.given_user("alice", main="main", clients=["client_a", "client_b"])
    s.given_event(
        "client_a", summary="Quarterly review",
        description="bring the deck",
        start="2026-02-02T09:00:00Z",
    )
    await s.run_reconciler("alice")

    # Full copy on main carries the real description, then the
    # source-calendar footer, then the managed tag.
    main_desc = _desc(s, "main", "Quarterly review")
    assert main_desc == f"bring the deck\n\n---\nSource: client_a\n\n{TAG}"

    # Peer busy block is tagged.
    peer_desc = _desc(s, "client_b", "Busy")
    assert peer_desc == TAG

    # The user's REAL event on the source calendar is untouched — no tag.
    src_desc = _desc(s, "client_a", "Quarterly review")
    assert src_desc == "bring the deck"
    assert TAG not in src_desc
    await s.close()


@pytest.mark.asyncio
async def test_edit_on_tagged_main_copy_does_not_leak_tag_to_source():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    ev = s.given_event(
        "client_a", summary="1:1",
        description="weekly sync",
        start="2026-02-02T09:00:00Z",
    )
    await s.run_reconciler("alice")

    # The user edits the description on the managed main copy.
    main_copy = next(e for e in s.list_events("main") if "1:1" in (e.get("summary") or ""))
    s.update_event("main", main_copy["id"], description="weekly sync — moved to Tuesdays")
    await s.run_reconciler("alice")

    # The edit reached the source, WITHOUT our tag.
    src_desc = _desc(s, "client_a", "1:1")
    assert src_desc == "weekly sync — moved to Tuesdays"
    assert TAG not in src_desc

    # The main copy still has exactly one tag (re-rendered, footer
    # re-applied), and the ledger stored the clean description (no tag,
    # no footer, no doubling).
    main_desc = _desc(s, "main", "1:1")
    assert main_desc == (
        f"weekly sync — moved to Tuesdays\n\n---\nSource: client_a\n\n{TAG}"
    )
    assert main_desc.count(TAG) == 1

    db = await s.setup_db()
    row = await (await db.execute(
        """SELECT description FROM ledger_events
            WHERE user_id = ? AND source_type = 'client'""",
        (user.user_id,),
    )).fetchone()
    assert row["description"] == "weekly sync — moved to Tuesdays"
    assert TAG not in (row["description"] or "")
    await s.close()


@pytest.mark.asyncio
async def test_reconcile_is_idempotent_with_the_tag():
    """The tag is part of the desired payload, so a second reconcile
    must not see drift and re-write everything."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    await s.given_user("alice", main="main", clients=["client_a", "client_b"])
    s.given_event("client_a", summary="Standup", start="2026-02-02T09:00:00Z")
    await s.run_reconciler("alice")
    out = await s.run_reconciler("alice")
    drain = out.get("drain", {}) if isinstance(out, dict) else {}
    # Nothing should have been enqueued/written on the second pass.
    assert drain.get("succeeded", 0) == 0, f"tag caused churn: {out}"
    await s.close()
