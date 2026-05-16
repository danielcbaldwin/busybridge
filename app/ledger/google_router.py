"""Per-account Google API routing.

A BusyBridge user connects several Google accounts: a *home*
account (whose calendar is the main calendar) plus one OAuth token
per connected client / personal calendar
(``client_calendars.oauth_token_id``).  A calendar can only be read
or written with the token of the account that has access to it —
calling the home token against a client account's calendar 403s /
404s.

:class:`GoogleRouter` implements the :class:`GoogleClient` protocol
by dispatching on ``calendar_id``: each call goes to the client for
the account that owns that calendar, falling back to the home
client for anything unmapped (e.g. the main calendar itself).  The
reconciler, ingest paths and outbox drain therefore need no changes
— they receive a router that quacks like one ``GoogleClient``.
"""

from __future__ import annotations

from app.ledger.google_client import GoogleClient


class GoogleRouter:
    """Routes each calendar's API calls to the account that can
    reach it.  Construct with the home client as ``default`` and a
    ``{google_calendar_id: GoogleClient}`` map for everything else.
    """

    def __init__(
        self,
        *,
        default: GoogleClient,
        by_calendar: dict[str, GoogleClient],
    ) -> None:
        self._default = default
        self._by_calendar = dict(by_calendar)

    def _client(self, calendar_id: str) -> GoogleClient:
        return self._by_calendar.get(calendar_id, self._default)

    # Every event-scoped call takes ``calendar_id`` first; dispatch
    # on it and forward the rest of the arguments verbatim.
    def insert_event(self, calendar_id, *args, **kwargs):
        return self._client(calendar_id).insert_event(calendar_id, *args, **kwargs)

    def get_event(self, calendar_id, *args, **kwargs):
        return self._client(calendar_id).get_event(calendar_id, *args, **kwargs)

    def update_event(self, calendar_id, *args, **kwargs):
        return self._client(calendar_id).update_event(calendar_id, *args, **kwargs)

    def patch_event(self, calendar_id, *args, **kwargs):
        return self._client(calendar_id).patch_event(calendar_id, *args, **kwargs)

    def delete_event(self, calendar_id, *args, **kwargs):
        return self._client(calendar_id).delete_event(calendar_id, *args, **kwargs)

    def list_events(self, calendar_id, *args, **kwargs):
        return self._client(calendar_id).list_events(calendar_id, *args, **kwargs)

    def list_instances(self, calendar_id, *args, **kwargs):
        return self._client(calendar_id).list_instances(calendar_id, *args, **kwargs)

    def list_calendar_list(self):
        # Not calendar-scoped — the home account's calendar list.
        return self._default.list_calendar_list()
