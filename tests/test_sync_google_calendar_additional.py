"""Coverage tests for the minimal legacy sync.google_calendar client.

Only the surviving surface is tested here: ``list_events``, the shared
retry wrapper (``execute_with_retry``), rate-limit classification, and
the module-level shared rate limiter.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.sync import google_calendar as module
from app.sync.google_calendar import (
    GoogleCalendarClient,
    _is_rate_limit_error,
    execute_with_retry,
)


class _RecordingLimiter:
    """No-op limiter that records acquire/backoff calls."""

    def __init__(self):
        self.acquires = 0
        self.backoffs: list[float] = []

    def acquire(self):
        self.acquires += 1

    def backoff(self, seconds: float):
        self.backoffs.append(seconds)


@pytest.fixture
def limiter(monkeypatch):
    """Replace the shared module-level limiter with a recording no-op."""
    fake = _RecordingLimiter()
    monkeypatch.setattr(module, "_rate_limiter", fake)
    return fake


class _FakeHttpError(Exception):
    def __init__(self, status: int, message: str = ""):
        self.resp = SimpleNamespace(status=status)
        self._message = message
        super().__init__(message or f"HTTP {status}")


# ---------------------------------------------------------------------------
# Rate-limit classification
# ---------------------------------------------------------------------------

def test_rate_limit_classification():
    """429 and quota-flavoured 403s are rate limits; other errors are not."""
    assert _is_rate_limit_error(_FakeHttpError(429)) is True
    assert _is_rate_limit_error(_FakeHttpError(403, "rateLimitExceeded")) is True
    assert _is_rate_limit_error(_FakeHttpError(403, "userRateLimitExceeded")) is True
    assert _is_rate_limit_error(_FakeHttpError(403, "quotaExceeded")) is True
    assert _is_rate_limit_error(_FakeHttpError(403, "dailyLimitExceeded")) is True
    assert _is_rate_limit_error(_FakeHttpError(403, "Rate Limit Exceeded")) is True
    # A permission-denied 403 must NOT be treated as retryable rate limit.
    assert _is_rate_limit_error(_FakeHttpError(403, "forbidden")) is False
    assert _is_rate_limit_error(_FakeHttpError(500, "boom")) is False
    assert _is_rate_limit_error(_FakeHttpError(404, "gone")) is False


# ---------------------------------------------------------------------------
# Shared module-level rate limiter
# ---------------------------------------------------------------------------

def test_rate_limiter_is_shared_across_instances_and_uses_settings(monkeypatch):
    """All clients share ONE limiter, parameterized from settings.

    backup.py / ics_export.py build one client per calendar; a
    per-instance limiter would multiply the documented global cap by
    the calendar count.
    """
    monkeypatch.setattr(module, "_rate_limiter", None)
    monkeypatch.setattr(
        module, "get_settings",
        lambda: SimpleNamespace(google_api_rate_limit_per_second=7.0),
    )

    first = module._get_rate_limiter()
    second = module._get_rate_limiter()
    assert first is second
    assert first._rate == 7.0

    # Two distinct client instances flow through the same limiter.
    acquired = []
    monkeypatch.setattr(
        type(first), "acquire", lambda self: acquired.append(id(self)),
    )
    for _ in range(2):
        client = object.__new__(GoogleCalendarClient)
        client.service = SimpleNamespace(
            events=lambda: SimpleNamespace(
                list=lambda **_kw: SimpleNamespace(execute=lambda: {"items": []})
            )
        )
        client.list_events("cal-1")
    assert acquired == [id(first), id(first)]


# ---------------------------------------------------------------------------
# execute_with_retry
# ---------------------------------------------------------------------------

def test_retry_on_server_error_then_success(monkeypatch, limiter):
    """500/502/503 are retried with backoff; the result is returned."""
    monkeypatch.setattr(module, "HttpError", _FakeHttpError)
    sleeps: list[float] = []
    monkeypatch.setattr(module.time, "sleep", sleeps.append)

    attempts = []

    def _execute():
        attempts.append(1)
        if len(attempts) < 3:
            raise _FakeHttpError(503)
        return {"ok": True}

    result = execute_with_retry(SimpleNamespace(execute=_execute))
    assert result == {"ok": True}
    assert len(attempts) == 3
    assert limiter.acquires == 3  # every attempt goes through the limiter
    assert sleeps == [1.0, 2.0]  # exponential backoff
    assert limiter.backoffs == []  # server errors don't pause other requests


def test_retry_on_rate_limit_imposes_global_backoff(monkeypatch, limiter):
    """A 429 retries AND pauses the shared limiter for other requests."""
    monkeypatch.setattr(module, "HttpError", _FakeHttpError)
    sleeps: list[float] = []
    monkeypatch.setattr(module.time, "sleep", sleeps.append)

    attempts = []

    def _execute():
        attempts.append(1)
        if len(attempts) < 2:
            raise _FakeHttpError(429)
        return {"ok": True}

    result = execute_with_retry(SimpleNamespace(execute=_execute))
    assert result == {"ok": True}
    assert limiter.backoffs == [4.0]  # base_delay * 2 ** (attempt + 2)
    assert sleeps == [4.0]


def test_non_retryable_error_raises_immediately(monkeypatch, limiter):
    """400/401/403-permission/404 fail on the first attempt, no sleeps."""
    monkeypatch.setattr(module, "HttpError", _FakeHttpError)
    sleeps: list[float] = []
    monkeypatch.setattr(module.time, "sleep", sleeps.append)

    def _execute():
        raise _FakeHttpError(404, "not found")

    with pytest.raises(_FakeHttpError):
        execute_with_retry(SimpleNamespace(execute=_execute))
    assert sleeps == []
    assert limiter.acquires == 1


def test_retries_exhausted_reraises(monkeypatch, limiter):
    """After max_retries the last error propagates."""
    monkeypatch.setattr(module, "HttpError", _FakeHttpError)
    monkeypatch.setattr(module.time, "sleep", lambda _s: None)

    attempts = []

    def _execute():
        attempts.append(1)
        raise _FakeHttpError(503)

    with pytest.raises(_FakeHttpError):
        execute_with_retry(SimpleNamespace(execute=_execute), max_retries=2)
    assert len(attempts) == 3  # initial + 2 retries


# ---------------------------------------------------------------------------
# list_events
# ---------------------------------------------------------------------------

def test_list_events_paginates(limiter):
    """list_events follows nextPageToken and concatenates pages."""

    class FakeEvents:
        def __init__(self):
            self.calls: list[dict] = []

        def list(self, **kwargs):
            self.calls.append(dict(kwargs))
            if kwargs.get("pageToken") == "page-2":
                return SimpleNamespace(
                    execute=lambda: {"items": [{"id": "evt-2"}]}
                )
            return SimpleNamespace(
                execute=lambda: {
                    "items": [{"id": "evt-1"}],
                    "nextPageToken": "page-2",
                }
            )

    events_api = FakeEvents()
    client = object.__new__(GoogleCalendarClient)
    client.service = SimpleNamespace(events=lambda: events_api)

    result = client.list_events("cal-1", single_events=False)
    assert [e["id"] for e in result["events"]] == ["evt-1", "evt-2"]
    # Full-sync params: always a bounded time range, never a syncToken.
    assert "syncToken" not in events_api.calls[0]
    assert "timeMin" in events_api.calls[0]
    assert "timeMax" in events_api.calls[0]


def test_list_events_http_error_mapping(monkeypatch, limiter):
    """403 → PermissionError, 404 → FileNotFoundError."""
    monkeypatch.setattr(module, "HttpError", _FakeHttpError)

    def _client_raising(status: int) -> GoogleCalendarClient:
        def _raise():
            raise _FakeHttpError(status, "forbidden")

        client = object.__new__(GoogleCalendarClient)
        client.service = SimpleNamespace(
            events=lambda: SimpleNamespace(
                list=lambda **_kw: SimpleNamespace(execute=_raise)
            )
        )
        return client

    with pytest.raises(PermissionError):
        _client_raising(403).list_events("cal-1")
    with pytest.raises(FileNotFoundError):
        _client_raising(404).list_events("cal-1")


def test_list_events_network_error_branch(limiter):
    """list_events should log and re-raise non-HttpError exceptions."""

    class ExplodingEvents:
        def list(self, **_kwargs):
            def _raise():
                raise RuntimeError("network timeout")

            return SimpleNamespace(execute=_raise)

    client = object.__new__(GoogleCalendarClient)
    client.service = SimpleNamespace(events=lambda: ExplodingEvents())

    with pytest.raises(RuntimeError):
        client.list_events("cal-1")


# ---------------------------------------------------------------------------
# Client construction
# ---------------------------------------------------------------------------

def test_google_calendar_client_service_carries_a_socket_timeout(monkeypatch):
    """The legacy GoogleCalendarClient builds its service through the
    shared timeout-bounded helper — a hung Google call must not tie up
    a worker thread forever (it previously accepted a `timeout` arg
    but never applied it)."""
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
