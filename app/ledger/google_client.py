"""Adapter layer between the ledger code and Google Calendar.

The ledger does not import :mod:`googleapiclient` directly; it
talks to a small protocol that both production code and the test
fakes implement.  This makes integration tests cheap (drop in a
:class:`tests.fakes.FakeGoogleCalendar`) and keeps real-Google
quirks isolated to one shim file.

The protocol intentionally mirrors the fake's flat surface
(``insert_event``, ``update_event``, ``delete_event``,
``list_events``, ...) rather than the ``service.events().list().execute()``
chain — production code wraps the chain inside
:class:`RealGoogleClient` (not implemented here yet; the cutover
work plugs in).
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class GoogleClient(Protocol):
    """The subset of Google Calendar API the ledger needs.

    Errors are raised with an HTTP status accessible via
    ``e.status`` (matching the test fake's ``GoogleApiError`` and
    a thin shim around ``googleapiclient.HttpError`` in
    production).  Network-layer failures are raised as a typed
    exception with the word ``Network`` in the class name —
    callers catch by isinstance(e, Exception) and inspect.
    """

    def insert_event(self, calendar_id: str, body: dict) -> dict: ...

    def get_event(self, calendar_id: str, event_id: str) -> dict: ...

    def update_event(
        self,
        calendar_id: str,
        event_id: str,
        body: dict,
        if_match: Optional[str] = None,
    ) -> dict: ...

    def patch_event(
        self,
        calendar_id: str,
        event_id: str,
        body: dict,
        if_match: Optional[str] = None,
        send_updates: Optional[str] = None,
    ) -> dict:
        """Field-scoped ``events.patch``.

        ``send_updates`` maps to the API's ``sendUpdates`` parameter
        (``'all'`` | ``'externalOnly'`` | ``'none'``).  ``None`` (the
        default) keeps the historical silent behaviour.  Only the
        origin-writeback patch ever passes a non-None value — see the
        notification policy note in
        :class:`~app.ledger.real_google_client.RealGoogleClient`.
        """
        ...

    def delete_event(
        self,
        calendar_id: str,
        event_id: str,
        if_match: Optional[str] = None,
    ) -> None: ...

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
    ) -> dict: ...

    def list_instances(
        self,
        calendar_id: str,
        event_id: str,
        show_deleted: bool = False,
        max_results: int = 250,
        page_token: Optional[str] = None,
    ) -> dict: ...

    def list_calendar_list(self) -> dict:
        """Return the user's CalendarList.

        Each item has at least ``id`` and may have ``primary: true``.
        Used by the OAuth-callback wiring in :mod:`app.auth.routes`
        to discover the user's main calendar.
        """
        ...
