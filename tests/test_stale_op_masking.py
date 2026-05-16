"""A successful op must record the hash IT applied, not the
projection's current desired hash.

If a projection's desired state changes (e.g. cleanup / pause sets it
to absent, at the same ledger version) while an older create/update
op is still in flight, the op still lands on Google.  Recording the
projection's *current* desired hash on success would mark the
projection "applied" at a state Google never received — so the diff
sees no divergence and never enqueues the corrective delete, leaving
the event orphaned.  The op carries the desired hash it was enqueued
for; success records that, leaving the projection diverged.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.ledger.outbox import _record_success
from tests.integration.framework import Scenario

UTC = timezone.utc
pytestmark = pytest.mark.asyncio


async def test_success_leaves_projection_diverged_when_desired_changed():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    ev = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, status, version,
               created_at, updated_at)
           VALUES (?, 'client:1:x', 'client', 'active', 1,
                   '2026-01-01', '2026-01-01') RETURNING id""",
        (user.user_id,),
    )).fetchone()
    proj = await (await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, desired_state,
               desired_payload_hash, desired_ledger_version, current_state)
           VALUES (?, 'main', 'present_full', 'create-hash', 1, 'absent')
           RETURNING id""",
        (int(ev["id"]),),
    )).fetchone()
    # A create op enqueued for desired hash 'create-hash'.
    op = await (await db.execute(
        """INSERT INTO outbox_operations
              (user_id, projection_id, operation, idempotency_key,
               ledger_version_at_enqueue, desired_payload_hash,
               target_google_calendar_id, payload_json, status, attempts)
           VALUES (?, ?, 'create', 'k1', 1, 'create-hash', 'main@cal',
                   '{"summary": "x"}', 'in_flight', 1)
           RETURNING *""",
        (user.user_id, int(proj["id"])),
    )).fetchone()

    # While the op is in flight, cleanup flips the projection to absent
    # at the SAME ledger version (no version bump).
    await db.execute(
        """UPDATE ledger_projections
              SET desired_state = 'absent', desired_payload_hash = 'absent'
            WHERE id = ?""",
        (int(proj["id"]),),
    )
    await db.commit()

    # The in-flight create op now lands successfully on Google.
    await _record_success(
        db, op,
        google_event_id="g1", google_etag="e1", now=datetime.now(UTC),
    )

    row = await (await db.execute(
        """SELECT applied_payload_hash, desired_payload_hash,
                  applied_ledger_version, desired_ledger_version
             FROM ledger_projections WHERE id = ?""",
        (int(proj["id"]),),
    )).fetchone()
    # The projection records what the op applied — the create hash —
    # NOT the since-changed 'absent'.  applied != desired, so the diff
    # will enqueue the corrective delete.
    assert row["applied_payload_hash"] == "create-hash"
    assert row["desired_payload_hash"] == "absent"
    assert row["applied_payload_hash"] != row["desired_payload_hash"], (
        "stale create was masked — no corrective delete would be enqueued"
    )
    await s.close()
