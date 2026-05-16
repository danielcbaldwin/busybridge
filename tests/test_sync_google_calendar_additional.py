"""Additional coverage tests for sync.google_calendar."""

from __future__ import annotations

from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_list_events_network_error_branch():
    """list_events should log and re-raise non-HttpError exceptions."""
    from app.sync.google_calendar import GoogleCalendarClient

    class ExplodingEvents:
        def list(self, **_kwargs):
            def _raise():
                raise RuntimeError("network timeout")

            return SimpleNamespace(execute=_raise)

    client = object.__new__(GoogleCalendarClient)
    client.settings = SimpleNamespace(calendar_sync_tag="syncTag", busy_block_title="Busy")
    client.service = SimpleNamespace(events=lambda: ExplodingEvents())

    with pytest.raises(RuntimeError):
        client.list_events("cal-1")


@pytest.mark.asyncio
async def test_search_events_pagination_and_http_error_mapping(monkeypatch):
    """search_events should paginate query results and map key HTTP errors."""
    from app.sync import google_calendar as module
    from app.sync.google_calendar import GoogleCalendarClient

    class FakeHttpError(Exception):
        def __init__(self, status: int):
            self.resp = SimpleNamespace(status=status)

    class FakeEvents:
        def __init__(self, status: int | None = None):
            self.status = status
            self.calls: list[dict] = []

        def list(self, **kwargs):
            self.calls.append(dict(kwargs))

            if self.status is not None:
                def _raise():
                    raise FakeHttpError(self.status)

                return SimpleNamespace(execute=_raise)

            if kwargs.get("pageToken") == "page-2":
                return SimpleNamespace(
                    execute=lambda: {"items": [{"id": "evt-2", "summary": "[BusyBridge] B"}]}
                )

            return SimpleNamespace(
                execute=lambda: {
                    "items": [{"id": "evt-1", "summary": "[BusyBridge] A"}],
                    "nextPageToken": "page-2",
                }
            )

    class FakeService:
        def __init__(self, events_api: FakeEvents):
            self.events_api = events_api

        def events(self):
            return self.events_api

    monkeypatch.setattr(module, "HttpError", FakeHttpError)

    list_api = FakeEvents()
    client = object.__new__(GoogleCalendarClient)
    client.settings = SimpleNamespace(calendar_sync_tag="syncTag", busy_block_title="Busy")
    client.service = FakeService(list_api)
    events = client.search_events("calendar-1", "[BusyBridge]")
    assert [event["id"] for event in events] == ["evt-1", "evt-2"]
    assert list_api.calls[0]["q"] == "[BusyBridge]"

    client.service = FakeService(FakeEvents(status=403))
    with pytest.raises(PermissionError):
        client.search_events("calendar-1", "x")

    client.service = FakeService(FakeEvents(status=404))
    with pytest.raises(FileNotFoundError):
        client.search_events("calendar-1", "x")


def test_can_user_edit_event_creator_self_branch():
    """can_user_edit_event should allow edits when creator.self is true."""
    from app.sync.google_calendar import can_user_edit_event

    event = {
        "creator": {"self": True},
        "organizer": {"email": "other@example.com"},
    }
    assert can_user_edit_event(event, "person@example.com") is True


def test_google_calendar_client_service_carries_a_socket_timeout(monkeypatch):
    """The legacy GoogleCalendarClient builds its service through the
    shared timeout-bounded helper — a hung Google call must not tie up
    a worker thread forever (it previously accepted a `timeout` arg
    but never applied it)."""
    from app.sync.google_calendar import GoogleCalendarClient

    captured: dict = {}

    def fake_build(*_args, **kwargs):
        captured["http"] = kwargs.get("http")
        return SimpleNamespace()

    monkeypatch.setattr("app.auth.google.build", fake_build)

    GoogleCalendarClient(access_token="tok", timeout=15)

    authed_http = captured["http"]
    assert authed_http is not None, "service built without a timeout http"
    # AuthorizedHttp wraps the httplib2.Http that carries the timeout.
    assert authed_http.http.timeout == 15
