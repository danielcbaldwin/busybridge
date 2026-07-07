"""``events.instances`` pagination in the full-sync cancellation scan.

Regression for the one-page instance scan: the full-sync
recurring-cancellation recovery scan
(``scan_full_sync_recurring_cancellations``) used to read only the
FIRST page of ``events.instances`` — the API paginates via
``nextPageToken``, so a daily series long enough to exceed one page
(2500 instances is ~7 years of a daily event) silently lost any
cancellation past page 1 while the sync token still advanced.

Covered here:

* The fake's ``list_instances`` paginates like the real endpoint
  (``nextPageToken`` + ``page_token``), including the small-page
  override tests use to force multi-page expansions.
* End-to-end: a cancelled instance that lands beyond page 1 of the
  instance scan is still recovered into the ledger, and the sync
  token advances.
* Failure accounting: a page-2+ fetch failure counts the parent's
  scan as failed, so the sync token is HELD BACK (NULL) and the next
  full sync retries — cancellations on the unfetched pages must not
  be stranded behind an advanced token.
"""

from __future__ import annotations

import pytest

from app.ledger.ingest.client import ingest_client_calendar
from tests.fakes.google_calendar import FakeGoogleCalendar, GoogleApiError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
PARENT_ID = "clientrec0001"
CAL_GOOGLE_ID = "cal-google-id"


def _daily_parent_body(count: int = 6) -> dict:
    return {
        "id": PARENT_ID,
        "summary": "Daily sync",
        "start": {"dateTime": "2026-06-01T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-06-01T09:30:00Z", "timeZone": "UTC"},
        "recurrence": [f"RRULE:FREQ=DAILY;COUNT={count}"],
    }


def _make_fake() -> FakeGoogleCalendar:
    fake = FakeGoogleCalendar()
    fake.add_calendar(CAL_GOOGLE_ID)
    fake.insert_event(CAL_GOOGLE_ID, _daily_parent_body())
    return fake


def _collect_all_pages(fake, **kwargs) -> list[list[dict]]:
    """Follow ``nextPageToken`` to exhaustion; returns items per page."""
    pages: list[list[dict]] = []
    page_token = None
    while True:
        resp = fake.list_instances(
            CAL_GOOGLE_ID, PARENT_ID, page_token=page_token, **kwargs,
        )
        pages.append(resp["items"])
        page_token = resp.get("nextPageToken")
        if not page_token:
            return pages


async def _seed_user(db) -> tuple[int, int]:
    """User + one client calendar; returns (user_id, client_calendar_id)."""
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id, main_calendar_id) "
        "VALUES ('u@x.com', 'g1', 'main-google-id')"
    )
    user_id = int(cur.lastrowid)
    await db.execute(
        "INSERT INTO main_calendar_sync_state (user_id) VALUES (?)", (user_id,)
    )
    cur = await db.execute(
        "INSERT INTO oauth_tokens "
        "(user_id, account_type, google_account_email, "
        " access_token_encrypted, refresh_token_encrypted) "
        "VALUES (?, 'client', 'u@x.com', ?, ?)",
        (user_id, b"x", b"y"),
    )
    tok_id = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO client_calendars "
        "(user_id, oauth_token_id, google_calendar_id, display_name) "
        "VALUES (?, ?, ?, 'Client')",
        (user_id, tok_id, CAL_GOOGLE_ID),
    )
    ccid = int(cur.lastrowid)
    await db.commit()
    return user_id, ccid


async def _cancelled_instance_rows(db, user_id: int) -> list:
    return await (await db.execute(
        "SELECT * FROM ledger_events "
        "WHERE user_id = ? AND parent_canonical_uid IS NOT NULL "
        "  AND status = 'cancelled'",
        (user_id,),
    )).fetchall()


async def _sync_token(db, ccid: int):
    row = await (await db.execute(
        "SELECT sync_token FROM calendar_sync_state WHERE client_calendar_id=?",
        (ccid,),
    )).fetchone()
    return row["sync_token"]


# ---------------------------------------------------------------------------
# Fake: list_instances pagination
# ---------------------------------------------------------------------------
def test_fake_list_instances_paginates_via_next_page_token():
    fake = _make_fake()

    pages = _collect_all_pages(fake, max_results=2)

    assert [len(p) for p in pages] == [2, 2, 2]
    ids = [i["id"] for page in pages for i in page]
    assert len(set(ids)) == 6

    # Single-page reference: same instances, same order.
    one_page = fake.list_instances(CAL_GOOGLE_ID, PARENT_ID, max_results=2500)
    assert "nextPageToken" not in one_page
    assert [i["id"] for i in one_page["items"]] == ids


def test_fake_instances_page_size_override_caps_max_results():
    """``instances_page_size`` forces small pages even when the caller
    asks for 2500 — this is how tests exercise the multi-page loop."""
    fake = _make_fake()
    fake.instances_page_size = 2

    resp = fake.list_instances(
        CAL_GOOGLE_ID, PARENT_ID, show_deleted=True, max_results=2500,
    )
    assert len(resp["items"]) == 2
    assert "nextPageToken" in resp


def test_fake_cancelled_instance_surfaces_on_a_later_page():
    fake = _make_fake()
    # Cancel day 5 of 6 — with page size 2 that is page 3.
    fake.delete_event(CAL_GOOGLE_ID, f"{PARENT_ID}_20260605T090000Z")
    fake.instances_page_size = 2

    pages = _collect_all_pages(fake, show_deleted=True, max_results=2500)

    cancelled_page_indexes = [
        idx for idx, page in enumerate(pages)
        for i in page if i.get("status") == "cancelled"
    ]
    assert cancelled_page_indexes and cancelled_page_indexes[0] >= 1, (
        "cancelled instance expected beyond page 1; got pages "
        f"{[[i['id'] for i in p] for p in pages]}"
    )


def test_fake_list_instances_rejects_unknown_page_token():
    fake = _make_fake()
    with pytest.raises(GoogleApiError) as exc:
        fake.list_instances(CAL_GOOGLE_ID, PARENT_ID, page_token="bogus")
    assert exc.value.status == 400


# ---------------------------------------------------------------------------
# Ingest: the cancellation scan follows every page
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_scan_recovers_cancellation_beyond_first_instances_page(test_db):
    """A cancellation on page 2+ of ``events.instances`` is still
    recovered by the full-sync scan, and the sync token advances."""
    db = test_db
    user_id, ccid = await _seed_user(db)

    fake = _make_fake()
    # Cancel the 2026-06-05 occurrence; with 2-item pages it sits on
    # page 3 of the instance expansion.
    fake.delete_event(CAL_GOOGLE_ID, f"{PARENT_ID}_20260605T090000Z")
    fake.instances_page_size = 2

    # Record the page tokens the scan sends, to prove it really
    # followed nextPageToken (the regression read page 1 only).
    seen_page_tokens: list = []
    orig_list_instances = fake.list_instances

    def recording(*args, **kwargs):
        seen_page_tokens.append(kwargs.get("page_token"))
        return orig_list_instances(*args, **kwargs)

    fake.list_instances = recording

    counters = await ingest_client_calendar(
        db, fake,
        user_id=user_id, client_calendar_id=ccid,
        google_calendar_id=CAL_GOOGLE_ID, user_email="u@x.com",
    )

    assert any(t is not None for t in seen_page_tokens), (
        "scan never followed nextPageToken — one-page regression"
    )
    assert counters["cancelled"] == 1

    rows = await _cancelled_instance_rows(db, user_id)
    assert len(rows) == 1
    assert rows[0]["recurrence_instance_original_start"].startswith("2026-06-05")

    # Fully-scanned pass: the sync token must advance.
    assert await _sync_token(db, ccid) is not None


class _PageTwoFailsGoogle:
    """Minimal GoogleClient whose instance scan fails on page 2.

    Page 1 carries one cancelled instance and a ``nextPageToken``;
    fetching page 2 raises.  ``list_events`` serves a single full-sync
    page containing the recurring parent, with a next sync token that
    the ingest would normally store.
    """

    def __init__(self):
        self.parent = _daily_parent_body()
        self.parent["status"] = "confirmed"

    def list_events(self, calendar_id, *, sync_token=None, page_token=None,
                    show_deleted=False, max_results=250):
        return {"items": [self.parent], "nextSyncToken": "TOK-NEW"}

    def list_instances(self, calendar_id, event_id, show_deleted=False,
                       max_results=250, page_token=None):
        if page_token is None:
            return {
                "items": [{
                    "id": f"{PARENT_ID}_20260602T090000Z",
                    "status": "cancelled",
                    "recurringEventId": PARENT_ID,
                    "originalStartTime": {"dateTime": "2026-06-02T09:00:00Z"},
                }],
                "nextPageToken": "page-2",
            }
        raise RuntimeError("simulated 503 fetching instances page 2")


@pytest.mark.asyncio
async def test_page_two_fetch_failure_holds_back_sync_token(test_db):
    """If a later instances page cannot be fetched, the scan counts as
    failed and the sync token stays NULL, so the next pass re-runs the
    full sync and retries the scan — cancellations on the unfetched
    pages must not be stranded behind an advanced token."""
    db = test_db
    user_id, ccid = await _seed_user(db)

    counters = await ingest_client_calendar(
        db, _PageTwoFailsGoogle(),
        user_id=user_id, client_calendar_id=ccid,
        google_calendar_id=CAL_GOOGLE_ID, user_email="u@x.com",
    )

    # Page 1's cancellation was still ingested (idempotent on retry)...
    assert counters["cancelled"] == 1
    rows = await _cancelled_instance_rows(db, user_id)
    assert len(rows) == 1

    # ...but the token is held back: NULL forces a fresh full sync
    # that re-attempts the scan.
    assert await _sync_token(db, ccid) is None, (
        "sync token advanced past a partially-scanned series — "
        "page-2+ cancellations would be silently stranded"
    )
