"""Production glue between the app's user/calendar DB tables and
the ledger pipeline.

This module is the *only* place that knows how to translate from
"a user_id in the app's database" into the bag of arguments
:func:`app.ledger.reconciler.reconcile_user` expects.  The
reconciler itself stays oblivious to the OAuth token store, the
``client_calendars.calendar_type`` discriminator, the webcal
subscription table, and anything else app-specific.

Entry points:

* :func:`reconcile_user_by_id` — load bindings + run one
  reconciliation pass.  Used by the webhook handler and the
  periodic scheduler job.
* :func:`drain_all_due_users` — find every user with a due
  ``reconcile_requests`` row, claim it, run, release.  Used by
  the scheduler tick.

Test injection:

* :func:`set_google_client_factory` — swap the
  ``RealGoogleClient(credentials)`` builder for a test factory
  that returns a :class:`tests.fakes.FakeGoogleCalendar`.  This
  is the only patch a test needs to make to run the full app
  against an in-memory fake.
* :func:`set_webcal_fetcher` — same idea for webcal subscription
  fetches.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

import aiosqlite

from app.auth.google import get_valid_access_token
from app.database import get_database
from app.ledger.reconciler import reconcile_user
from app.ledger.real_google_client import RealGoogleClient
from app.ledger.triggers import claim_due_request, release_request
from google.oauth2.credentials import Credentials

logger = logging.getLogger(__name__)
UTC = timezone.utc


# ---------------------------------------------------------------------------
# Pluggable client factory (tests override this)
# ---------------------------------------------------------------------------
GoogleClientFactory = Callable[[int, str], Awaitable[Any]]
WebcalFetcher = Callable[[str, Optional[str]], dict]


async def _default_google_client_factory(user_id: int, email: str):
    """Production factory: build a RealGoogleClient from the
    OAuth token store."""
    access_token = await get_valid_access_token(user_id, email)
    return RealGoogleClient(Credentials(token=access_token))


_google_client_factory: GoogleClientFactory = _default_google_client_factory
_webcal_fetcher: Optional[WebcalFetcher] = None


def set_google_client_factory(factory: Optional[GoogleClientFactory]) -> None:
    """Override the GoogleClient builder.  Pass ``None`` to reset.

    Test usage::

        from app.ledger.runtime import set_google_client_factory

        async def fake_factory(user_id, email):
            return my_fake_google_calendar

        set_google_client_factory(fake_factory)
    """
    global _google_client_factory
    _google_client_factory = (
        factory if factory is not None else _default_google_client_factory
    )


def set_webcal_fetcher(fetcher: Optional[WebcalFetcher]) -> None:
    """Override the webcal fetch hook.  Pass ``None`` to reset
    to the httpx-backed default."""
    global _webcal_fetcher
    _webcal_fetcher = fetcher


def _resolve_webcal_fetcher() -> Optional[WebcalFetcher]:
    if _webcal_fetcher is not None:
        return _webcal_fetcher
    try:
        return _default_webcal_fetcher()
    except Exception as e:  # httpx may be unavailable in some envs
        logger.warning("default webcal fetcher unavailable: %s", e)
        return None


async def reconcile_user_by_id(
    user_id: int,
    *,
    include_main: bool = True,
    drain: bool = True,
    run_discovery: bool = False,
) -> dict:
    """Load this user's calendars + tokens, build a RealGoogleClient,
    and run one reconciliation pass.

    Returns the reconciler's counters dict.  Callers (webhook
    handler, scheduler) typically log the result and move on.

    When ``settings.ledger_dry_run`` is True the outbox is never
    drained regardless of the ``drain`` argument — the run
    ingests + plans + diffs only, leaving pending outbox rows as
    a preview of what WOULD be written to Google.
    """
    from app.config import get_settings as _gs
    if _gs().ledger_dry_run:
        drain = False

    db = await get_database()
    user = await _load_user(db, user_id)
    if user is None:
        raise ValueError(f"user {user_id} not found")
    if user["main_calendar_id"] is None:
        # User hasn't finished OOBE; skip.
        return {"skipped": "no_main_calendar"}

    # Get an OAuth token for the user's home account → main calendar.
    main_email = await _resolve_home_email(db, user_id)
    if not main_email:
        return {"skipped": "no_home_oauth_token"}
    try:
        main_client = await _google_client_factory(user_id, main_email)
    except Exception as e:
        logger.warning(
            "Cannot build Google client for user %s: %s", user_id, e,
        )
        return {"skipped": "google_client_unavailable", "error": str(e)}

    # Active client calendars.
    client_rows = await (await db.execute(
        """SELECT id, google_calendar_id, oauth_token_id, calendar_type
             FROM client_calendars
            WHERE user_id = ? AND is_active = 1""",
        (user_id,),
    )).fetchall()
    active_clients: list[dict] = []
    active_personals: list[dict] = []
    for row in client_rows:
        entry = {"id": int(row["id"]), "google_calendar_id": row["google_calendar_id"]}
        if (row["calendar_type"] or "client") == "personal":
            active_personals.append(entry)
        else:
            active_clients.append(entry)

    # All known calendars (active + disconnected) for the diff/outbox
    # to be able to issue deletes against disconnected calendars.
    all_known_rows = await (await db.execute(
        """SELECT id, google_calendar_id
             FROM client_calendars
            WHERE user_id = ?""",
        (user_id,),
    )).fetchall()
    all_known = [
        {"id": int(r["id"]), "google_calendar_id": r["google_calendar_id"]}
        for r in all_known_rows
    ]

    # Webcal subscriptions.
    webcal_rows = await (await db.execute(
        """SELECT id, url FROM webcal_subscriptions
            WHERE user_id = ? AND is_active = 1""",
        (user_id,),
    )).fetchall()
    webcal_subs = [
        {"id": int(r["id"]), "url": r["url"]} for r in webcal_rows
    ]

    return await reconcile_user(
        db, main_client,
        user_id=user_id,
        user_email=main_email,
        main_google_calendar_id=user["main_calendar_id"],
        client_calendars=active_clients,
        all_known_client_calendars=all_known,
        personal_calendars=active_personals,
        webcal_subscriptions=webcal_subs,
        webcal_fetch=_resolve_webcal_fetcher() if webcal_subs else None,
        include_main=include_main,
        drain=drain,
        run_discovery=run_discovery,
    )


async def drain_all_due_users(*, now: Optional[datetime] = None) -> dict:
    """Pull every user with a due ``reconcile_requests`` row,
    run their reconciler, and release the row.

    Returns ``{user_id: counters}`` for every user processed
    (empty when nothing is due).
    """
    now = now or datetime.now(UTC)
    db = await get_database()
    rows = await (await db.execute(
        """SELECT user_id FROM reconcile_requests
            WHERE in_flight = 0
              AND (scheduled_for IS NULL OR scheduled_for <= ?)""",
        (now.isoformat(),),
    )).fetchall()

    out: dict[int, Any] = {}
    for row in rows:
        user_id = int(row["user_id"])
        claim = await claim_due_request(db, user_id=user_id, now=now)
        if claim is None:
            continue
        try:
            out[user_id] = await reconcile_user_by_id(user_id)
        except Exception as e:
            logger.exception("reconcile user %s failed: %s", user_id, e)
            out[user_id] = {"error": str(e)}
        finally:
            await release_request(db, user_id=user_id)
    return out


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
async def _load_user(db: aiosqlite.Connection, user_id: int) -> Optional[aiosqlite.Row]:
    return await (await db.execute(
        "SELECT * FROM users WHERE id = ?", (user_id,),
    )).fetchone()


async def _resolve_home_email(db: aiosqlite.Connection, user_id: int) -> Optional[str]:
    """Return the email of the user's 'home' OAuth account, used to
    talk to their main calendar."""
    row = await (await db.execute(
        """SELECT google_account_email
             FROM oauth_tokens
            WHERE user_id = ? AND account_type = 'home'
            ORDER BY id DESC LIMIT 1""",
        (user_id,),
    )).fetchone()
    if row is None:
        return None
    return row["google_account_email"]


def _default_webcal_fetcher():
    """Build the default httpx-backed webcal fetch hook.

    Implemented inline rather than imported eagerly so test
    environments without httpx (or without SSRF guards configured)
    can still import this module.
    """
    import httpx

    def fetch(url: str, if_none_match: Optional[str]) -> dict:
        headers = {}
        if if_none_match:
            headers["If-None-Match"] = if_none_match
        try:
            r = httpx.get(url, headers=headers, timeout=30, follow_redirects=True)
        except httpx.HTTPError as e:
            raise RuntimeError(f"webcal fetch failed: {e}") from e
        return {
            "status": r.status_code,
            "etag": r.headers.get("ETag"),
            "body": r.content if r.status_code == 200 else None,
        }

    return fetch
