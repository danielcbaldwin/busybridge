"""RealGoogleClient — conditional-modification headers.

Google Calendar API v3 does ETag-gated writes via the ``If-Match``
HTTP header (returning 412 on mismatch).  The body-level ``etag``
field is output-only and is NOT a write precondition — so the
outbox's etag-gating only works if the real adapter sends a real
``If-Match`` header.
"""

from __future__ import annotations

from unittest.mock import MagicMock


class _FakeRequest:
    """Stands in for a googleapiclient HttpRequest: a mutable
    ``headers`` dict and an ``execute()`` that returns a result."""

    def __init__(self, result):
        self.headers: dict = {}
        self._result = result

    def execute(self):
        return self._result


def _client(monkeypatch, *, verbs: dict):
    """Build a RealGoogleClient whose service returns _FakeRequests.

    ``verbs`` maps an events verb ('update'/'patch'/'delete') to the
    result its execute() should return.  Returns
    ``(client, service, {verb: _FakeRequest})``.
    """
    from app.ledger import real_google_client as rgc

    service = MagicMock()
    requests: dict = {}
    for verb, result in verbs.items():
        req = _FakeRequest(result)
        requests[verb] = req
        getattr(service.events.return_value, verb).return_value = req
    monkeypatch.setattr(rgc, "build", lambda *a, **k: service)
    client = rgc.RealGoogleClient(MagicMock())
    return client, service, requests


def test_update_event_sends_if_match_as_an_http_header(monkeypatch):
    client, service, reqs = _client(
        monkeypatch, verbs={"update": {"id": "e1", "etag": "new"}},
    )
    client.update_event("cal", "e1", {"summary": "x"}, if_match='W/"abc"')
    assert reqs["update"].headers.get("If-Match") == 'W/"abc"'


def test_update_event_does_not_put_etag_in_the_body(monkeypatch):
    """The read-only etag must not leak into the request body."""
    client, service, reqs = _client(
        monkeypatch, verbs={"update": {"id": "e1"}},
    )
    client.update_event("cal", "e1", {"summary": "x"}, if_match='W/"abc"')
    body = service.events.return_value.update.call_args.kwargs["body"]
    assert "etag" not in body


def test_update_event_without_if_match_sends_no_header(monkeypatch):
    client, service, reqs = _client(
        monkeypatch, verbs={"update": {"id": "e1"}},
    )
    client.update_event("cal", "e1", {"summary": "x"})
    assert "If-Match" not in reqs["update"].headers


def test_patch_event_sends_if_match_header(monkeypatch):
    client, service, reqs = _client(
        monkeypatch, verbs={"patch": {"id": "e1"}},
    )
    client.patch_event("cal", "e1", {"attendees": []}, if_match='W/"xyz"')
    assert reqs["patch"].headers.get("If-Match") == 'W/"xyz"'


def test_delete_event_sends_if_match_header(monkeypatch):
    client, service, reqs = _client(
        monkeypatch, verbs={"delete": None},
    )
    client.delete_event("cal", "e1", if_match='W/"del"')
    assert reqs["delete"].headers.get("If-Match") == 'W/"del"'


def test_delete_event_without_if_match_sends_no_header(monkeypatch):
    client, service, reqs = _client(
        monkeypatch, verbs={"delete": None},
    )
    client.delete_event("cal", "e1")
    assert "If-Match" not in reqs["delete"].headers
