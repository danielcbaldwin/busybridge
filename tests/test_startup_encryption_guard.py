"""Startup must refuse to run with the wrong encryption key.

A structurally-valid but incorrect 32-byte key loads without error
yet cannot decrypt stored credentials.  The lifespan decrypts a
stored organization credential to catch this — AES-GCM raises
InvalidTag on a key mismatch — and aborts rather than booting a
broken instance that would fail on every OAuth token.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.asyncio


async def test_lifespan_aborts_on_wrong_encryption_key(
    test_db, tmp_path, monkeypatch,
):
    import app.encryption as enc_mod
    import app.main as main
    from app.database import get_database
    from app.encryption import EncryptionManager

    # An organization credential encrypted with key A.
    enc_a = EncryptionManager(b"A" * 32)
    db = await get_database()
    await db.execute(
        """INSERT INTO organization
              (google_workspace_domain, google_client_id_encrypted,
               google_client_secret_encrypted)
           VALUES (?, ?, ?)""",
        ("example.com", enc_a.encrypt("client-id"), enc_a.encrypt("secret")),
    )
    await db.commit()

    # The lifespan loads a DIFFERENT key.
    key_file = tmp_path / "enc.key"
    key_file.write_bytes(b"B" * 32)
    monkeypatch.setattr(main, "get_settings", lambda: SimpleNamespace(
        public_url="http://localhost:3000",
        database_path=":memory:",
        encryption_key_file=str(key_file),
    ))
    monkeypatch.setattr("app.config.get_encryption_key", lambda: b"B" * 32)

    saved_manager = enc_mod._encryption_manager
    try:
        with pytest.raises(SystemExit):
            async with main.lifespan(main.app):
                pass
    finally:
        enc_mod._encryption_manager = saved_manager
