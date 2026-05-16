"""Disconnecting a calendar must drop its webhook channels.

The webhook-renewal job selects channels purely by expiry — it does
not filter on ``client_calendars.is_active`` — so a disconnected
calendar whose channel rows survive would be renewed forever.
``disconnect_calendar`` deletes them.
"""

from __future__ import annotations

import pytest

from app.ledger.admin_ops import disconnect_calendar
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def test_disconnect_calendar_deletes_its_webhook_channels():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("work")
    user = await s.given_user("alice", main="main", clients=["work"])
    db = await s.setup_db()
    work_id = user.client_calendar_ids["work"]

    await db.execute(
        """INSERT INTO webhook_channels
              (user_id, calendar_type, client_calendar_id,
               channel_id, resource_id, expiration)
           VALUES (?, 'client', ?, 'ch-1', 'res-1', '2099-01-01')""",
        (user.user_id, work_id),
    )
    await db.commit()

    await disconnect_calendar(
        db, user_id=user.user_id, client_calendar_id=work_id,
    )

    row = await (await db.execute(
        "SELECT COUNT(*) AS n FROM webhook_channels WHERE client_calendar_id = ?",
        (work_id,),
    )).fetchone()
    assert row["n"] == 0, "disconnect left a webhook channel behind"
    await s.close()
