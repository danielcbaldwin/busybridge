"""Proxy IP headers must only be trusted when configured.

On a direct deployment any client can send ``X-Real-IP`` /
``X-Forwarded-For``; trusting them unconditionally lets an attacker
mint a fresh rate-limit bucket per forged IP.  ``_get_real_ip`` honours
them only when ``settings.trust_proxy_headers`` is set.
"""

from __future__ import annotations

import app.rate_limit as rate_limit


class _FakeClient:
    def __init__(self, host):
        self.host = host


class _FakeRequest:
    def __init__(self, headers, client_host="9.9.9.9"):
        self.headers = headers
        self.client = _FakeClient(client_host)


def _settings(trust):
    return type("S", (), {"trust_proxy_headers": trust})()


def test_proxy_headers_ignored_when_not_trusted(monkeypatch):
    monkeypatch.setattr(rate_limit, "get_settings", lambda: _settings(False))
    req = _FakeRequest(
        {"X-Real-IP": "1.2.3.4", "X-Forwarded-For": "5.6.7.8"},
        client_host="9.9.9.9",
    )
    # Forged headers are ignored — the real connection IP keys the limit.
    assert rate_limit._get_real_ip(req) == "9.9.9.9"


def test_x_real_ip_used_when_trusted(monkeypatch):
    monkeypatch.setattr(rate_limit, "get_settings", lambda: _settings(True))
    req = _FakeRequest({"X-Real-IP": "1.2.3.4"}, client_host="9.9.9.9")
    assert rate_limit._get_real_ip(req) == "1.2.3.4"


def test_forwarded_for_rightmost_used_when_trusted(monkeypatch):
    monkeypatch.setattr(rate_limit, "get_settings", lambda: _settings(True))
    req = _FakeRequest(
        {"X-Forwarded-For": "1.1.1.1, 2.2.2.2, 3.3.3.3"},
        client_host="9.9.9.9",
    )
    # Rightmost entry — the one the trusted proxy appended.
    assert rate_limit._get_real_ip(req) == "3.3.3.3"


def test_falls_back_to_connection_ip_when_no_headers(monkeypatch):
    monkeypatch.setattr(rate_limit, "get_settings", lambda: _settings(True))
    req = _FakeRequest({}, client_host="9.9.9.9")
    assert rate_limit._get_real_ip(req) == "9.9.9.9"
