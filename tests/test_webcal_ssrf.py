"""The scheduled webcal poll must apply the SSRF guard.

Subscription *creation* validates the feed URL against private /
reserved networks.  The scheduled ledger poll runs indefinitely
later — a feed can redirect or DNS-rebind to internal
infrastructure after the creation-time check — so the default
webcal fetch hook must validate every poll too.

The default hook delegates to ``app.utils.ics_fetch.fetch_ics_feed``,
which rejects a blocked address with a "blocked address" ValueError;
the hook surfaces that as a RuntimeError.  A raw ``httpx.get`` (the
old behaviour) would instead attempt the connection and fail — if
at all — with a transport error that never mentions "blocked".
"""

from __future__ import annotations

import socket

import pytest

from app.ledger.runtime import _default_webcal_fetcher

# pytest-asyncio runs in auto mode — async tests need no explicit mark.


def _fake_getaddrinfo(ip: str):
    """A getaddrinfo stand-in that resolves any hostname to ``ip``."""
    def _resolver(host, *_args, **_kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 0))]
    return _resolver


@pytest.mark.parametrize("url", [
    "http://127.0.0.1/feed.ics",            # loopback
    "http://169.254.169.254/latest/meta",   # cloud metadata / link-local
    "http://10.0.0.5/internal.ics",         # RFC 1918
    "http://192.168.1.1/feed.ics",          # RFC 1918
    "http://[::1]/feed.ics",                # IPv6 loopback
])
async def test_default_webcal_fetcher_rejects_private_addresses(url):
    fetch = _default_webcal_fetcher()
    with pytest.raises(RuntimeError) as exc:
        await fetch(url, None)
    # The rejection came from the SSRF validator, not from a
    # connection error — proving the guard ran before any fetch.
    assert "blocked" in str(exc.value).lower(), (
        f"expected an SSRF-block rejection, got: {exc.value}"
    )


async def test_fetch_ics_feed_rejects_redirect_to_internal_host(monkeypatch):
    """A feed that passes the entry-URL check but 302-redirects to an
    internal address must be rejected AT the redirect hop.

    httpx's own follow_redirects validates only the final URL; the
    fetcher follows redirects manually so every hop is checked.
    """
    import httpx

    from app.utils import ics_fetch

    def handler(request):
        # The (public) entry URL redirects straight at cloud metadata.
        return httpx.Response(
            302, headers={"Location": "http://169.254.169.254/latest/meta"}
        )

    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient

    def client_with_mock(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(ics_fetch.httpx, "AsyncClient", client_with_mock)

    # A public IP literal passes the entry-URL SSRF check hermetically
    # (no DNS); the redirect target is the link-local metadata host.
    with pytest.raises(ValueError) as exc:
        await ics_fetch.fetch_ics_feed("http://93.184.216.34/feed.ics")
    assert "blocked" in str(exc.value).lower(), (
        f"expected the redirect hop to be SSRF-blocked, got: {exc.value}"
    )


def test_validate_url_accepts_a_normal_domain(monkeypatch):
    """A normal domain-based feed URL must be accepted — the SSRF
    guard must not reject every hostname that is not an IP literal."""
    from app.utils import ics_fetch

    monkeypatch.setattr(
        ics_fetch.socket, "getaddrinfo", _fake_getaddrinfo("93.184.216.34"),
    )
    # Must not raise.
    ics_fetch.validate_url_for_ssrf("https://feeds.example.com/calendar.ics")


def test_validate_url_rejects_domain_resolving_to_private_ip(monkeypatch):
    """A hostname that resolves to an internal address is rejected,
    even though the hostname itself is not an IP literal."""
    from app.utils import ics_fetch

    monkeypatch.setattr(
        ics_fetch.socket, "getaddrinfo", _fake_getaddrinfo("10.0.0.5"),
    )
    with pytest.raises(ValueError, match="blocked"):
        ics_fetch.validate_url_for_ssrf("https://internal.example.com/cal.ics")


async def test_fetch_ics_feed_succeeds_for_a_public_domain(monkeypatch):
    """A domain-based feed resolving to a public IP fetches normally —
    the connection is pinned to the validated IP."""
    import httpx

    from app.utils import ics_fetch

    monkeypatch.setattr(
        ics_fetch.socket, "getaddrinfo", _fake_getaddrinfo("93.184.216.34"),
    )

    def handler(request):
        # The connection is pinned to the validated IP, with the real
        # hostname carried in the Host header.
        assert request.url.host == "93.184.216.34"
        assert request.headers["host"] == "feeds.example.com"
        return httpx.Response(200, text="BEGIN:VCALENDAR\nEND:VCALENDAR")

    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        ics_fetch.httpx, "AsyncClient",
        lambda *a, **kw: real_client(*a, **{**kw, "transport": transport}),
    )

    body, _etag = await ics_fetch.fetch_ics_feed(
        "https://feeds.example.com/calendar.ics"
    )
    assert body is not None and "VCALENDAR" in body
