"""Production adapter from the :class:`GoogleClient` protocol to
``googleapiclient.discovery.build()``.

The fake (``tests.fakes.FakeGoogleCalendar``) and this real client
share an identical surface so the ledger code (planner, diff,
outbox, ingest) is oblivious to which one is plugged in.

Two non-trivial mappings happen here:

* ``googleapiclient`` raises ``HttpError`` with ``e.resp.status``
  whereas the ledger code reads ``e.status``.  We wrap every
  ``HttpError`` in our own :class:`GoogleApiError` (shape-
  compatible with the fake's) before bubbling.
* ``If-Match`` is sent via the request's HTTP headers, which
  googleapiclient exposes through the underlying ``http`` object.
  We pass it on the per-call ``HttpRequest`` using the
  ``execute(...).resp`` mechanism is not available — instead we
  set ``etag`` on the body which Google accepts as an equivalent
  precondition for update/patch/delete.

The wrapper is intentionally thin; idempotent retry and backoff
already live in the existing ``app/sync/google_calendar.py``.
For the ledger we rely on the outbox to drive retries from the
top level, so this adapter raises plainly and lets the outbox
decide.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Error shim (shape-compatible with tests.fakes.GoogleApiError)
# ---------------------------------------------------------------------------
class GoogleApiError(Exception):
    """Same shape as ``tests.fakes.google_calendar.GoogleApiError``.

    Surfaced so the outbox / ingest can read ``e.status`` regardless
    of whether the underlying client is the fake or the real one.
    """

    def __init__(self, status: int, reason: str, message: str = ""):
        self.status = status
        self.reason = reason
        self.message = message
        super().__init__(
            f"HTTP {status} {reason}: {message}" if message else f"HTTP {status} {reason}"
        )


def _wrap(e: HttpError) -> GoogleApiError:
    """Convert a googleapiclient HttpError into our error type."""
    status_code = e.resp.status
    reason = e.resp.reason or ""
    try:
        message = e._get_reason() or str(e)
    except Exception:  # pragma: no cover
        message = str(e)
    return GoogleApiError(int(status_code), reason, message)


# ---------------------------------------------------------------------------
# RealGoogleClient
# ---------------------------------------------------------------------------
class RealGoogleClient:
    """Production implementation of the GoogleClient protocol.

    Constructed per-user from an :class:`google.oauth2.credentials.Credentials`
    instance.  All calls translate one-to-one to ``googleapiclient``'s
    ``service.events().<verb>(...).execute()`` chain.
    """

    def __init__(self, credentials: Credentials):
        self._service = build(
            "calendar", "v3",
            credentials=credentials,
            cache_discovery=False,
        )

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------
    def insert_event(self, calendar_id: str, body: dict) -> dict:
        try:
            return self._service.events().insert(
                calendarId=calendar_id,
                body=body,
                conferenceDataVersion=1,
                sendNotifications=False,
            ).execute()
        except HttpError as e:
            raise _wrap(e) from e

    def get_event(self, calendar_id: str, event_id: str) -> dict:
        try:
            return self._service.events().get(
                calendarId=calendar_id, eventId=event_id,
            ).execute()
        except HttpError as e:
            raise _wrap(e) from e

    def update_event(
        self,
        calendar_id: str,
        event_id: str,
        body: dict,
        if_match: Optional[str] = None,
    ) -> dict:
        # Google honours the body-level ``etag`` field as an
        # ``If-Match`` precondition — we put it there because
        # googleapiclient doesn't expose per-call HTTP headers
        # cleanly.  An empty or absent value disables the check.
        body = dict(body)
        if if_match:
            body["etag"] = if_match
        try:
            return self._service.events().update(
                calendarId=calendar_id,
                eventId=event_id,
                body=body,
                conferenceDataVersion=1,
                sendNotifications=False,
            ).execute()
        except HttpError as e:
            raise _wrap(e) from e

    def patch_event(
        self,
        calendar_id: str,
        event_id: str,
        body: dict,
        if_match: Optional[str] = None,
    ) -> dict:
        body = dict(body)
        if if_match:
            body["etag"] = if_match
        try:
            return self._service.events().patch(
                calendarId=calendar_id,
                eventId=event_id,
                body=body,
                conferenceDataVersion=1,
                sendNotifications=False,
            ).execute()
        except HttpError as e:
            raise _wrap(e) from e

    def delete_event(
        self,
        calendar_id: str,
        event_id: str,
        if_match: Optional[str] = None,
    ) -> None:
        try:
            self._service.events().delete(
                calendarId=calendar_id,
                eventId=event_id,
                sendNotifications=False,
            ).execute()
        except HttpError as e:
            raise _wrap(e) from e

    def list_events(
        self,
        calendar_id: str,
        sync_token: Optional[str] = None,
        time_min: Optional[datetime | str] = None,
        time_max: Optional[datetime | str] = None,
        max_results: int = 250,
        single_events: bool = False,
        page_token: Optional[str] = None,
        show_deleted: bool = False,
    ) -> dict:
        params: dict = {
            "calendarId": calendar_id,
            "maxResults": max_results,
            "singleEvents": single_events,
            "showDeleted": show_deleted,
        }
        if sync_token is not None:
            params["syncToken"] = sync_token
        if page_token is not None:
            params["pageToken"] = page_token
        if time_min is not None:
            params["timeMin"] = _to_iso(time_min)
        if time_max is not None:
            params["timeMax"] = _to_iso(time_max)
        try:
            return self._service.events().list(**params).execute()
        except HttpError as e:
            raise _wrap(e) from e

    def list_instances(
        self,
        calendar_id: str,
        event_id: str,
        show_deleted: bool = False,
        max_results: int = 250,
    ) -> dict:
        try:
            return self._service.events().instances(
                calendarId=calendar_id,
                eventId=event_id,
                showDeleted=show_deleted,
                maxResults=max_results,
            ).execute()
        except HttpError as e:
            raise _wrap(e) from e


def _to_iso(v: datetime | str) -> str:
    if isinstance(v, str):
        return v
    if v.tzinfo is None:
        return v.isoformat() + "Z"
    return v.isoformat()
