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
from app.ledger.google_router import GoogleRouter
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
WebcalFetcher = Callable[[str, Optional[str]], Awaitable[dict]]


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

    # Resolve every calendar + the account (OAuth token) that can
    # reach it, and build a router that sends each calendar's API
    # calls to the right account.
    access = await build_user_google_access(db, user_id)
    if access is None:
        return {"skipped": "no_home_oauth_token"}

    # Webcal subscriptions (HTTP, not account-routed).
    webcal_rows = await (await db.execute(
        """SELECT id, url FROM webcal_subscriptions
            WHERE user_id = ? AND is_active = 1""",
        (user_id,),
    )).fetchall()
    webcal_subs = [
        {"id": int(r["id"]), "url": r["url"]} for r in webcal_rows
    ]

    return await reconcile_user(
        db, access["router"],
        user_id=user_id,
        user_email=access["main_email"],
        main_google_calendar_id=user["main_calendar_id"],
        client_calendars=access["client_calendars"],
        all_known_client_calendars=access["all_known"],
        personal_calendars=access["personal_calendars"],
        webcal_subscriptions=webcal_subs,
        webcal_fetch=_resolve_webcal_fetcher() if webcal_subs else None,
        include_main=include_main,
        drain=drain,
        run_discovery=run_discovery,
    )


async def build_user_google_access(
    db: aiosqlite.Connection, user_id: int,
) -> Optional[dict]:
    """Resolve a user's calendars and the account that reaches each.

    A BusyBridge user has several Google accounts — a home account
    (main calendar) and one OAuth token per connected client /
    personal calendar (``client_calendars.oauth_token_id``).  Each
    calendar is only reachable with ITS account's token; the home
    token 403s against another account's calendar.

    Returns ``None`` when the user has no usable home OAuth token.
    Otherwise a dict with:

    * ``main_email`` — the home account's email.
    * ``router`` — a :class:`GoogleRouter` that dispatches each
      calendar to the right account's client.
    * ``client_calendars`` / ``personal_calendars`` — the *active,
      reachable* calendars to ingest.
    * ``all_known`` — every client_calendars row (active +
      disconnected) for the diff/outbox calendar-id mapping.
    """
    main_email = await _resolve_home_email(db, user_id)
    if not main_email:
        return None
    try:
        home_client = await _google_client_factory(user_id, main_email)
    except Exception as e:
        logger.warning(
            "Cannot build Google client for user %s home account: %s",
            user_id, e,
        )
        return None

    # Every client/personal calendar joined to the account that owns it.
    cal_rows = await (await db.execute(
        """SELECT cc.id, cc.google_calendar_id, cc.calendar_type,
                  cc.is_active, ot.google_account_email
             FROM client_calendars cc
             JOIN oauth_tokens ot ON ot.id = cc.oauth_token_id
            WHERE cc.user_id = ?""",
        (user_id,),
    )).fetchall()

    # Build one client per distinct account; route each calendar to it.
    clients_by_email: dict[str, Any] = {main_email: home_client}
    by_calendar: dict[str, Any] = {}
    unreachable: set[str] = set()
    for row in cal_rows:
        email = row["google_account_email"]
        if email not in clients_by_email:
            try:
                clients_by_email[email] = await _google_client_factory(
                    user_id, email,
                )
            except Exception as e:
                logger.warning(
                    "Cannot build Google client for user %s account %s: %s",
                    user_id, email, e,
                )
                clients_by_email[email] = None
        client = clients_by_email[email]
        if client is None:
            unreachable.add(row["google_calendar_id"])
        else:
            by_calendar[row["google_calendar_id"]] = client

    router = GoogleRouter(default=home_client, by_calendar=by_calendar)

    # Ingest only active calendars whose account is reachable.
    active_clients: list[dict] = []
    active_personals: list[dict] = []
    for row in cal_rows:
        if not row["is_active"]:
            continue
        if row["google_calendar_id"] in unreachable:
            logger.warning(
                "skipping calendar %s for user %s: its account's OAuth "
                "token is unavailable",
                row["google_calendar_id"], user_id,
            )
            continue
        entry = {
            "id": int(row["id"]),
            "google_calendar_id": row["google_calendar_id"],
        }
        if (row["calendar_type"] or "client") == "personal":
            active_personals.append(entry)
        else:
            active_clients.append(entry)

    all_known = [
        {"id": int(r["id"]), "google_calendar_id": r["google_calendar_id"]}
        for r in cal_rows
    ]
    return {
        "main_email": main_email,
        "router": router,
        "client_calendars": active_clients,
        "personal_calendars": active_personals,
        "all_known": all_known,
    }


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


def _default_webcal_fetcher() -> WebcalFetcher:
    """Build the default SSRF-safe webcal fetch hook.

    Delegates to :func:`app.utils.ics_fetch.fetch_ics_feed`, the
    same SSRF-guarded fetch subscription-creation uses: it validates
    the URL — and the post-redirect URL — against private / reserved
    networks.  A raw ``httpx.get`` here would let a feed redirect or
    DNS-rebind to internal infrastructure *after* the creation-time
    check (the scheduled poll happens indefinitely later).

    Async so the 30s HTTP fetch does not block the reconciler's
    event loop.
    """
    import httpx

    from app.utils.ics_fetch import fetch_ics_feed

    async def fetch(url: str, if_none_match: Optional[str]) -> dict:
        try:
            content, new_etag = await fetch_ics_feed(
                url, etag=if_none_match, timeout=30.0,
            )
        except httpx.HTTPStatusError as e:
            # A real HTTP error response — surface the status so the
            # webcal ingest records a fetch failure.
            return {
                "status": e.response.status_code,
                "etag": None,
                "body": None,
            }
        except (httpx.HTTPError, ValueError) as e:
            # ValueError → the URL was SSRF-blocked; HTTPError →
            # transport failure.  Either way the poll failed.
            raise RuntimeError(f"webcal fetch failed: {e}") from e
        if content is None:
            # 304 Not Modified — keep the prior etag.
            return {"status": 304, "etag": if_none_match, "body": None}
        return {
            "status": 200,
            "etag": new_etag,
            "body": content.encode("utf-8"),
        }

    return fetch
