"""The session cookie's Secure flag must follow the deployment scheme.

Hard-coding ``Secure=True`` silently breaks login on the plain-HTTP
deployments a self-hosted tool legitimately runs — a LAN box, a
localhost trial, a setup behind an HTTP-reached TLS proxy.  The
browser never returns a Secure cookie over HTTP, so the user can never
stay logged in.  ``session_cookie_secure`` derives the flag from the
configured public URL.
"""

from __future__ import annotations

import app.auth.session as session_mod
from app.auth.session import session_cookie_secure


class _FakeSettings:
    def __init__(self, public_url: str):
        self.public_url = public_url


def test_secure_flag_off_for_http_deployment(monkeypatch):
    monkeypatch.setattr(
        session_mod, "get_settings",
        lambda: _FakeSettings("http://busybridge.lan:3000"),
    )
    assert session_cookie_secure() is False


def test_secure_flag_on_for_https_deployment(monkeypatch):
    monkeypatch.setattr(
        session_mod, "get_settings",
        lambda: _FakeSettings("https://busybridge.example.com"),
    )
    assert session_cookie_secure() is True


def test_secure_flag_is_case_insensitive(monkeypatch):
    monkeypatch.setattr(
        session_mod, "get_settings",
        lambda: _FakeSettings("HTTPS://Busybridge.Example.Com"),
    )
    assert session_cookie_secure() is True
