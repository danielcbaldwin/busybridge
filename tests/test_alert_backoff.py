"""Alert retries must honour an exponential backoff and escalate
once an alert exhausts its attempts.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pytest

from app.database import get_database
from app.jobs.alerts import process_alert_queue

pytestmark = pytest.mark.asyncio


async def test_recently_failed_alert_is_held_off_by_backoff(test_db, monkeypatch):
    """An alert that failed seconds ago must not be retried until its
    backoff window ((1 << attempts) minutes) has elapsed."""
    db = await get_database()
    recent = (datetime.utcnow() - timedelta(seconds=30)).isoformat()
    old = (datetime.utcnow() - timedelta(minutes=10)).isoformat()
    await db.execute(
        "INSERT INTO alert_queue "
        "(alert_type, recipient_email, subject, body, attempts, last_attempt) "
        "VALUES ('sync_failures', 'held@example.com', 's', 'b', 1, ?)",
        (recent,),
    )
    await db.execute(
        "INSERT INTO alert_queue "
        "(alert_type, recipient_email, subject, body, attempts, last_attempt) "
        "VALUES ('sync_failures', 'due@example.com', 's', 'b', 1, ?)",
        (old,),
    )
    await db.commit()

    sent: list[str] = []

    async def fake_send(**kwargs):
        sent.append(kwargs["to_email"])

    monkeypatch.setattr("app.alerts.email.send_email", fake_send)
    await process_alert_queue()

    assert "due@example.com" in sent, "an alert past its backoff must retry"
    assert "held@example.com" not in sent, (
        "an alert still inside its 2-minute backoff must not retry"
    )


async def test_alert_escalates_on_permanent_failure(test_db, monkeypatch, caplog):
    """The third consecutive failure must be logged at ERROR so a lost
    alert is visible to an operator."""
    db = await get_database()
    await db.execute(
        "INSERT INTO alert_queue "
        "(alert_type, recipient_email, subject, body, attempts) "
        "VALUES ('token_revoked', 'dead@example.com', 's', 'b', 2)"
    )
    await db.commit()

    async def fake_fail(**_kwargs):
        raise RuntimeError("smtp down")

    monkeypatch.setattr("app.alerts.email.send_email", fake_fail)
    with caplog.at_level(logging.ERROR):
        await process_alert_queue()

    assert any(
        "PERMANENTLY FAILED" in r.getMessage() for r in caplog.records
    ), "exhausting retries must escalate at ERROR level"
