"""Per-account Google routing.

A BusyBridge user connects several Google accounts: a home account
(main calendar) plus one OAuth token per connected client/personal
calendar.  Each calendar is reachable only with ITS account's
token — the home token 403s/404s against another account's
calendar.  ``reconcile_user_by_id`` must build a ``GoogleRouter``
that sends every calendar's API calls to the right account.
"""

from __future__ import annotations

import pytest

from app.database import get_database
from app.ledger.google_router import GoogleRouter
from app.ledger.runtime import reconcile_user_by_id, set_google_client_factory
from tests.fakes.google_calendar import FakeGoogleCalendar, GoogleApiError

pytestmark = pytest.mark.asyncio


def _timed(summary: str) -> dict:
    return {
        "summary": summary,
        "start": {"dateTime": "2026-07-01T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-07-01T09:30:00Z", "timeZone": "UTC"},
    }


class _ScopedGoogle:
    """A view of a shared FakeGoogleCalendar limited to the calendars
    one account may access — models real Google rejecting a token
    with no access to a calendar (404)."""

    def __init__(self, shared: FakeGoogleCalendar, allowed: set[str]):
        self._shared = shared
        self._allowed = set(allowed)

    def _check(self, calendar_id: str) -> None:
        if calendar_id not in self._allowed:
            raise GoogleApiError(
                404, "Not Found",
                f"calendar {calendar_id} not accessible by this account",
            )

    def insert_event(self, c, *a, **k):
        self._check(c)
        return self._shared.insert_event(c, *a, **k)

    def get_event(self, c, *a, **k):
        self._check(c)
        return self._shared.get_event(c, *a, **k)

    def update_event(self, c, *a, **k):
        self._check(c)
        return self._shared.update_event(c, *a, **k)

    def patch_event(self, c, *a, **k):
        self._check(c)
        return self._shared.patch_event(c, *a, **k)

    def delete_event(self, c, *a, **k):
        self._check(c)
        return self._shared.delete_event(c, *a, **k)

    def list_events(self, c, *a, **k):
        self._check(c)
        return self._shared.list_events(c, *a, **k)

    def list_instances(self, c, *a, **k):
        self._check(c)
        return self._shared.list_instances(c, *a, **k)

    def list_calendar_list(self):
        return {"items": [{"id": c} for c in sorted(self._allowed)]}


async def test_google_router_dispatches_each_calendar_to_its_client():
    acct_home = FakeGoogleCalendar()
    acct_home.add_calendar("main@home")
    acct_work = FakeGoogleCalendar()
    acct_work.add_calendar("cal@work")

    router = GoogleRouter(
        default=acct_home, by_calendar={"cal@work": acct_work},
    )
    # A write to the work calendar lands in the work account...
    router.insert_event("cal@work", _timed("Work"))
    # ...and a write to main lands in the home account (the default).
    router.insert_event("main@home", _timed("Home"))

    assert [e["summary"] for e in acct_work.list_events("cal@work")["items"]] == [
        "Work",
    ]
    assert [e["summary"] for e in acct_home.list_events("main@home")["items"]] == [
        "Home",
    ]
    # The home account never received the work calendar's event.
    assert acct_home.event_count("main@home") == 1


async def test_reconcile_routes_each_calendar_to_its_own_account(test_db):
    """End-to-end: a client calendar on a *separate* Google account
    is ingested with that account's token and projected to main.
    Under the old single-token code the home token would 404 on the
    work calendar and the event would never reach main."""
    db = await get_database()

    uid = int((await (await db.execute(
        """INSERT INTO users
              (email, google_user_id, display_name, main_calendar_id)
           VALUES ('u@home.test', 'g-home', 'U', 'main@home.test')
           RETURNING id""",
    )).fetchone())["id"])
    await db.execute(
        """INSERT INTO oauth_tokens
              (user_id, account_type, google_account_email,
               access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'home', 'u@home.test', ?, ?)""",
        (uid, b"x", b"y"),
    )
    work_tok = int((await (await db.execute(
        """INSERT INTO oauth_tokens
              (user_id, account_type, google_account_email,
               access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', 'u@work.test', ?, ?) RETURNING id""",
        (uid, b"x", b"y"),
    )).fetchone())["id"])
    await db.execute(
        """INSERT INTO client_calendars
              (user_id, oauth_token_id, google_calendar_id,
               display_name, calendar_type, is_active)
           VALUES (?, ?, 'work@cal.test', 'Work', 'client', 1)""",
        (uid, work_tok),
    )
    await db.commit()

    # One shared "Google": the work event lives on the work calendar.
    shared = FakeGoogleCalendar()
    shared.add_calendar("main@home.test")
    shared.add_calendar("work@cal.test")
    shared.insert_event("work@cal.test", _timed("Work meeting"))

    # Each account's token reaches only its own calendars.
    views = {
        "u@home.test": _ScopedGoogle(shared, {"main@home.test"}),
        "u@work.test": _ScopedGoogle(shared, {"work@cal.test"}),
    }

    async def factory(user_id, email):
        return views[email]

    set_google_client_factory(factory)
    try:
        out = await reconcile_user_by_id(uid)
    finally:
        set_google_client_factory(None)

    assert "skipped" not in out, f"reconcile was skipped: {out}"
    main_summaries = [
        e["summary"] for e in shared.list_events("main@home.test")["items"]
    ]
    assert "Work meeting" in main_summaries, (
        "the work calendar's event did not reach main — it was not "
        f"ingested with the work account's token; main has {main_summaries}"
    )
