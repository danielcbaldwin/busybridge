"""Backups must be trustworthy: the DB copy is integrity-checked and
streamed (not read whole into memory), and an insufficient-disk backup is
refused rather than left as a silently-truncated, corrupt file you only
discover is bad at restore time.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import zipfile

import pytest

import app.sync.backup as backup_mod
from app.config import get_settings
from app.sync.backup import create_backup, get_backup_dir

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _isolated_backup_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("BACKUP_PATH", str(tmp_path / "backups"))
    yield


async def test_backup_db_entry_is_a_valid_restorable_sqlite_db(test_db):
    meta = await create_backup()
    zip_path = os.path.join(get_backup_dir(), f"{meta['backup_id']}.zip")
    assert os.path.exists(zip_path)

    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        assert "database.db" in names and "metadata.json" in names
        data = zf.read("database.db")

    # The streamed copy must be a valid, openable SQLite database that
    # passes its own integrity check — i.e. actually restorable.
    fd, p = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        with open(p, "wb") as f:
            f.write(data)
        conn = sqlite3.connect(p)
        try:
            assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        finally:
            conn.close()
    finally:
        os.unlink(p)


async def test_backup_refused_when_disk_too_small_and_leaves_no_zip(
    test_db, monkeypatch
):
    settings = get_settings()
    db_path = settings.database_path

    real_exists = os.path.exists
    real_getsize = os.path.getsize
    monkeypatch.setattr(
        backup_mod.os.path, "exists",
        lambda p: True if p == db_path else real_exists(p))
    monkeypatch.setattr(
        backup_mod.os.path, "getsize",
        lambda p: 10 ** 9 if p == db_path else real_getsize(p))

    class _Tiny:
        f_bavail = 1
        f_frsize = 1
    monkeypatch.setattr(backup_mod.os, "statvfs", lambda p: _Tiny())

    with pytest.raises(RuntimeError, match="refusing to back up"):
        await create_backup()

    # No partial/corrupt zip left behind.
    backup_dir = get_backup_dir()
    leftovers = (
        [f for f in os.listdir(backup_dir) if f.endswith(".zip")]
        if os.path.exists(backup_dir) else []
    )
    assert leftovers == [], f"a refused backup left a zip behind: {leftovers}"
