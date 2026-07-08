"""Minimal legacy Google Calendar client — backup / ICS export only.

This module is the surviving remnant of the pre-ledger sync engine.
Its ONLY production consumers are :mod:`app.sync.backup` and
:mod:`app.sync.ics_export`, which use :class:`AsyncGoogleCalendarClient`
for two things:

* ``list_events`` — full-range event snapshots of a calendar.
* ``is_our_event`` — classifying events as BusyBridge-managed
  (delegates to :func:`app.ledger.identity.is_busybridge_event`).

Do NOT add new callers.  New code must use the ledger stack's client
(``app/ledger/real_google_client.py`` via ``app/ledger/async_google.py``
and ``app/ledger/google_router.py``) instead.
"""

import asyncio
import functools
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Optional

from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError

from app.config import get_settings

logger = logging.getLogger(__name__)

# Server errors worth retrying (rate-limit errors are handled separately).
_RETRYABLE_SERVER_STATUSES = {500, 502, 503}

# Google encodes quota / rate-limit errors as one of these reason codes,
# carried in an HTTP 403 (classic) or 429 response.  Lowercase substring
# checks — same classification as app/ledger/outbox.py.
_RATE_LIMIT_TOKENS = (
    "ratelimitexceeded",
    "userratelimitexceeded",
    "quotaexceeded",
    "dailylimitexceeded",
    "rate limit exceeded",
)


def _is_rate_limit_error(error: HttpError) -> bool:
    """Check if an HttpError is a rate-limit (not a permission) error."""
    if error.resp.status == 429:
        return True
    if error.resp.status == 403:
        msg = str(error).lower()
        return any(tok in msg for tok in _RATE_LIMIT_TOKENS)
    return False


class _RateLimiter:
    """Thread-safe token-bucket rate limiter for Google API calls.

    Limits sustained request rate and supports a global backoff window
    that pauses all requests after a rate-limit response.
    """

    def __init__(self, rate: float = 5.0, burst: int = 5):
        """
        Args:
            rate: Sustained requests per second.
            burst: Maximum burst tokens (allows short bursts above rate).
        """
        self._rate = rate
        self._burst = burst
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()
        self._backoff_until = 0.0

    def acquire(self):
        """Block until a request slot is available."""
        while True:
            with self._lock:
                now = time.monotonic()
                if now < self._backoff_until:
                    wait = self._backoff_until - now
                else:
                    elapsed = now - self._last_refill
                    self._tokens = min(
                        self._burst, self._tokens + elapsed * self._rate,
                    )
                    self._last_refill = now
                    if self._tokens >= 1.0:
                        self._tokens -= 1.0
                        return
                    wait = (1.0 - self._tokens) / self._rate
            time.sleep(wait)

    def backoff(self, seconds: float):
        """Impose a global pause (e.g. after a rate-limit response)."""
        with self._lock:
            target = time.monotonic() + seconds
            if target > self._backoff_until:
                self._backoff_until = target


# Process-wide limiter shared by every GoogleCalendarClient instance
# (and by app/auth/google.py's fetch_* helpers).  backup.py and
# ics_export.py construct one client per calendar, so a per-instance
# limiter would multiply the documented cap by the calendar count —
# the limiter MUST be module-level to actually enforce a global rate.
# Created lazily so importing this module never requires settings.
# Tests may swap it out by monkeypatching ``_rate_limiter``.
_rate_limiter: Optional[_RateLimiter] = None
_rate_limiter_init_lock = threading.Lock()


def _get_rate_limiter() -> _RateLimiter:
    """Return the shared limiter, creating it from settings on first use."""
    global _rate_limiter
    if _rate_limiter is None:
        with _rate_limiter_init_lock:
            if _rate_limiter is None:
                rate = get_settings().google_api_rate_limit_per_second
                _rate_limiter = _RateLimiter(
                    rate=rate, burst=max(1, int(rate)),
                )
    return _rate_limiter


def execute_with_retry(request, max_retries: int = 5, base_delay: float = 1.0):
    """Execute a Google API request with rate limiting and exponential backoff.

    Rate-limits all outgoing requests through the shared module-level
    limiter to stay within Google's per-user quota.  Retries on
    rate-limit errors (403 rateLimitExceeded, 429) and transient server
    errors (500, 502, 503) with exponential backoff.  All other errors
    (400, 401, 403 permission-denied, 404, etc.) fail immediately.
    """
    limiter = _get_rate_limiter()
    for attempt in range(max_retries + 1):
        limiter.acquire()
        try:
            return request.execute()
        except HttpError as e:
            is_rate_limit = _is_rate_limit_error(e)
            is_server_error = e.resp.status in _RETRYABLE_SERVER_STATUSES

            if not (is_rate_limit or is_server_error) or attempt == max_retries:
                raise

            if is_rate_limit:
                # Longer backoff for rate limits; also pause other requests
                delay = min(base_delay * (2 ** (attempt + 2)), 60)
                limiter.backoff(delay)
            else:
                delay = min(base_delay * (2 ** attempt), 30)

            logger.warning(
                "Google API %s%s (attempt %d/%d), retrying in %.1fs",
                e.resp.status,
                " rate-limited" if is_rate_limit else "",
                attempt + 1, max_retries, delay,
            )
            time.sleep(delay)


class GoogleCalendarClient:
    """Read-only wrapper around Google Calendar API for backup/export."""

    def __init__(self, access_token=None, *, credentials=None, settings=None, timeout: int = 30):
        """
        Initialize with access token or pre-built credentials.

        Args:
            access_token: Google OAuth access token (positional, for backward compat)
            credentials: Pre-built credentials object (e.g. service account)
            settings: Settings object (optional, will use get_settings() if not provided)
            timeout: Request timeout in seconds (default: 30)
        """
        if credentials is not None:
            self.credentials = credentials
        elif access_token is not None:
            self.credentials = Credentials(token=access_token)
        else:
            raise ValueError("Either access_token or credentials must be provided")
        # Build through the shared helper so this service carries the
        # same socket timeout as every other Google call — a hung
        # connection must not tie up a worker thread indefinitely.
        # (``timeout`` was previously accepted but never applied.)
        from app.auth.google import build_calendar_service
        self.service = build_calendar_service(self.credentials, timeout=timeout)
        self.settings = settings or get_settings()

    def _execute_with_retry(self, request, max_retries: int = 5, base_delay: float = 1.0):
        """Execute a request via the shared retry + rate-limit wrapper."""
        return execute_with_retry(
            request, max_retries=max_retries, base_delay=base_delay,
        )

    def list_events(
        self,
        calendar_id: str,
        time_min: Optional[datetime] = None,
        time_max: Optional[datetime] = None,
        max_results: int = 2500,
        single_events: bool = False,
    ) -> dict:
        """List events from a calendar over a time range (full sync).

        Defaults to last month → next year when no range is given.
        """
        try:
            # Full sync - get events from last month to next year
            if not time_min:
                time_min = datetime.utcnow() - timedelta(days=30)
            if not time_max:
                time_max = datetime.utcnow() + timedelta(days=365)

            request_params = {
                "calendarId": calendar_id,
                "maxResults": max_results,
                "singleEvents": single_events,
                "timeMin": time_min.isoformat() + "Z",
                "timeMax": time_max.isoformat() + "Z",
            }

            all_events = []
            page_token = None

            while True:
                if page_token:
                    request_params["pageToken"] = page_token

                result = self._execute_with_retry(
                    self.service.events().list(**request_params),
                )
                all_events.extend(result.get("items", []))

                page_token = result.get("nextPageToken")
                if not page_token:
                    break

            return {"events": all_events}

        except HttpError as e:
            if e.resp.status == 403:
                # Permission denied - calendar access revoked
                logger.error(f"Permission denied for calendar {calendar_id}")
                raise PermissionError(f"Access to calendar {calendar_id} was revoked")
            elif e.resp.status == 404:
                # Calendar deleted
                logger.warning(f"Calendar {calendar_id} not found (may have been deleted)")
                raise FileNotFoundError(f"Calendar {calendar_id} not found")
            else:
                # Other HTTP errors
                logger.error(f"HTTP error {e.resp.status} fetching events for calendar {calendar_id}")
                raise
        except Exception as e:
            # Network errors, timeouts, etc.
            logger.error(f"Network error fetching events for calendar {calendar_id}: {type(e).__name__}")
            raise

    def is_our_event(self, event: dict) -> bool:
        """Check if an event was written by BusyBridge.

        Delegates to the single shared predicate in
        :func:`app.ledger.identity.is_busybridge_event` so the backup
        snapshot and the ICS "clean" export can never disagree about
        what counts as ours.
        """
        # Late import to avoid a hard dependency from app/sync/ →
        # app/ledger/ (the latter is the post-cutover home).
        from app.ledger.identity import is_busybridge_event

        return is_busybridge_event(
            event,
            sync_tag=self.settings.calendar_sync_tag,
            managed_prefix=self.settings.managed_event_prefix,
        )


class AsyncGoogleCalendarClient:
    """Async wrapper around GoogleCalendarClient.

    Offloads all blocking Google API calls to a thread pool via
    ``asyncio.to_thread`` so they never block the event loop.  Non-I/O
    methods (like ``is_our_event``) are passed through directly.
    """

    # Methods that perform network I/O and must run in a thread.
    _IO_METHODS = frozenset({"list_events"})

    def __init__(self, *args, **kwargs):
        self._sync = GoogleCalendarClient(*args, **kwargs)

    def __getattr__(self, name):
        attr = getattr(self._sync, name)
        if name in self._IO_METHODS and callable(attr):
            @functools.wraps(attr)
            async def _async(*args, **kwargs):
                return await asyncio.to_thread(attr, *args, **kwargs)
            # Cache so subsequent accesses skip __getattr__
            object.__setattr__(self, name, _async)
            return _async
        return attr
