"""OOBE hardening: the wizard binds to one browser from request one,
gates the encryption-key step behind the earlier steps, and a direct
jump to step 6 can never mint or reveal a key.
"""

from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from app.ui import setup as oobe


async def _oobe_incomplete() -> bool:
    return False


def _request(path: str = "/setup") -> Request:
    """A GET request carrying whatever OOBE session cookie is current."""
    headers = []
    token = oobe._oobe_data.get("_session_token")
    if token:
        headers.append(
            (b"cookie", f"{oobe._OOBE_COOKIE}={token}".encode())
        )
    return Request({
        "type": "http", "method": "GET", "path": path, "headers": headers,
    })


class _FormReq:
    def __init__(self, form: dict):
        self._form = form

    @property
    def cookies(self) -> dict:
        token = oobe._oobe_data.get("_session_token")
        return {oobe._OOBE_COOKIE: token} if token else {}

    async def form(self):
        return self._form


@pytest.mark.asyncio
async def test_direct_step6_get_does_not_mint_or_reveal_a_key(test_db, monkeypatch):
    """A fresh browser jumping straight to /setup?step=6 must be
    redirected to an earlier step — never shown or issued a key."""
    monkeypatch.setattr("app.ui.setup.is_oobe_completed", _oobe_incomplete)
    resp = await oobe.setup_wizard(_request("/setup"), step=6)
    assert resp.status_code == 302
    assert resp.headers["location"] == "/setup?step=2"
    assert "encryption_key" not in oobe._oobe_data
    assert "encryption_key_b64" not in oobe._oobe_data


@pytest.mark.asyncio
async def test_direct_step6_post_is_rejected_without_prior_steps(test_db, monkeypatch):
    """A direct POST /setup/step/6 with no prior steps must 400 — it
    must not write a key file or seed an organization/admin."""
    monkeypatch.setattr("app.ui.setup.is_oobe_completed", _oobe_incomplete)
    with pytest.raises(HTTPException) as exc:
        await oobe.setup_step_6(_FormReq({"confirmed": "on"}))
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_concurrent_first_requests_only_one_binds(test_db, monkeypatch):
    """Two concurrent first visitors race to bind the wizard; the
    process lock must let exactly one bind and reject the other."""
    monkeypatch.setattr("app.ui.setup.is_oobe_completed", _oobe_incomplete)
    # Both requests are built while the wizard is unbound — neither
    # carries a cookie.
    req_a, req_b = _request("/setup"), _request("/setup")
    results = await asyncio.gather(
        oobe.setup_wizard(req_a, step=1),
        oobe.setup_wizard(req_b, step=1),
        return_exceptions=True,
    )
    ok = [r for r in results if not isinstance(r, Exception)]
    rejected = [r for r in results if isinstance(r, HTTPException)]
    assert len(ok) == 1, "exactly one concurrent first request may bind"
    assert len(rejected) == 1 and rejected[0].status_code == 403
    assert oobe._oobe_data.get("_session_token")
