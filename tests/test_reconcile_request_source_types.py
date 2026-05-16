"""reconcile_requests.sources_json must never mix ints and strings.

Two writers touch the column:

* the ingest layer (``ingest.client._record_affected``) and admin ops
  (``admin_ops._append_affected``) store integer ledger-event ids;
* the trigger path (``enqueue_webhook`` / ``enqueue_periodic`` /
  ``enqueue_manual``) used to merge a *string* source hint into it.

Once both had written, ``_upsert_request``'s ``sorted(set(...))``
raised ``TypeError: '<' not supported between instances of 'str' and
'int'`` — a crash on the hot webhook path.  The trigger path no longer
writes the column at all; these tests pin that.
"""

from __future__ import annotations

import json

import pytest

from app.ledger.ingest.client import _record_affected
from app.ledger.triggers import enqueue_periodic, enqueue_webhook
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def test_webhook_after_ingest_recorded_ids_does_not_crash():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    uid = user.user_id

    # Ingest recorded three dirty ledger rows as integer ids.
    await _record_affected(db, user_id=uid, ledger_ids=[11, 22, 33])
    await db.commit()

    # A webhook (string hint) then a periodic tick arrive — the very
    # sequence that used to raise TypeError on the merge.
    await enqueue_webhook(db, user_id=uid, source_hint="client:7")
    await enqueue_periodic(db, user_id=uid)

    row = await (await db.execute(
        "SELECT sources_json FROM reconcile_requests WHERE user_id = ?",
        (uid,),
    )).fetchone()
    stored = json.loads(row["sources_json"])
    # The integer ids survive untouched; no string hint contaminated them.
    assert sorted(stored) == [11, 22, 33]
    assert all(isinstance(x, int) for x in stored)
    await s.close()


async def test_trigger_path_does_not_write_sources_json():
    """A bare trigger (no prior ingest) leaves sources_json NULL."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    await enqueue_webhook(db, user_id=user.user_id, source_hint="main")

    row = await (await db.execute(
        "SELECT sources_json FROM reconcile_requests WHERE user_id = ?",
        (user.user_id,),
    )).fetchone()
    assert row is not None, "trigger did not create a reconcile request"
    assert row["sources_json"] is None
    await s.close()
