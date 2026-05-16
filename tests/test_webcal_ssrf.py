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

import pytest

from app.ledger.runtime import _default_webcal_fetcher

pytestmark = pytest.mark.asyncio


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
