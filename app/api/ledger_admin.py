"""Admin endpoints backed by the ledger pipeline.

These run alongside the legacy admin endpoints in ``app/api/admin.py``.
They expose the new system's state through ``app.ledger.facade`` and
let admins drive ``app.ledger.admin_ops`` (recolor, cleanup, pause,
disconnect, full re-sync) without going through the legacy paths.

Mounted at ``/api/admin/ledger``.  All endpoints require admin auth.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.auth.session import User, require_admin
from app.database import get_database
from app.ledger import admin_ops, facade

router = APIRouter(prefix="/admin/ledger", tags=["admin", "ledger"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------
class LedgerHealth(BaseModel):
    user_id: int
    event_counts_by_source: dict[str, int]
    main_copies: int
    busy_blocks_by_calendar: dict[int, int]
    outbox: dict[str, int]
    sync_failures: dict


class PermanentFailure(BaseModel):
    projection_id: int
    ledger_event_id: int
    target_kind: str
    target_calendar_id: Optional[int]
    last_error: Optional[str]
    last_attempt_at: Optional[str]
    summary: Optional[str]
    canonical_uid: Optional[str]


# ---------------------------------------------------------------------------
# Read endpoints
# ---------------------------------------------------------------------------
@router.get("/health/{user_id}", response_model=LedgerHealth)
async def get_ledger_health(
    user_id: int,
    _: User = Depends(require_admin),
) -> LedgerHealth:
    """One-shot operational snapshot for one user.

    Combines the four facade aggregators into a single dashboard
    payload.  Useful when an admin needs to know "is the ledger in
    a healthy state for this user right now?" without paginating
    through the per-projection table.
    """
    db = await get_database()
    return LedgerHealth(
        user_id=user_id,
        event_counts_by_source=await facade.count_events_for_user(db, user_id=user_id),
        main_copies=await facade.count_main_copies(db, user_id=user_id),
        busy_blocks_by_calendar=await facade.count_busy_blocks_per_calendar(
            db, user_id=user_id,
        ),
        outbox=await facade.outbox_summary(db, user_id=user_id),
        sync_failures=await facade.sync_failure_status(db, user_id=user_id),
    )


@router.get("/permanent-failures/{user_id}", response_model=list[PermanentFailure])
async def list_permanent_failures(
    user_id: int,
    limit: int = 50,
    _: User = Depends(require_admin),
) -> list[PermanentFailure]:
    """Projections that hit the poison-pill threshold.  Each one is
    a specific event the admin needs to look at by hand."""
    db = await get_database()
    rows = await facade.list_permanent_failures(db, user_id=user_id, limit=limit)
    return [
        PermanentFailure(
            projection_id=int(r.get("id") or 0),
            ledger_event_id=int(r.get("ledger_event_id") or 0),
            target_kind=str(r.get("target_kind") or ""),
            target_calendar_id=(
                int(r["target_calendar_id"])
                if r.get("target_calendar_id") is not None
                else None
            ),
            last_error=r.get("last_error"),
            last_attempt_at=r.get("last_attempt_at"),
            summary=r.get("summary"),
            canonical_uid=r.get("canonical_uid"),
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Admin operations
# ---------------------------------------------------------------------------
@router.post("/users/{user_id}/recolor-calendar/{client_calendar_id}")
async def recolor_calendar(
    user_id: int,
    client_calendar_id: int,
    new_color_id: Optional[str] = None,
    _: User = Depends(require_admin),
) -> dict:
    """Change a client calendar's color and bump every sourced
    ledger row so projections re-render on the next reconcile."""
    db = await get_database()
    n = await admin_ops.recolor_client_calendar(
        db,
        client_calendar_id=client_calendar_id,
        new_color_id=new_color_id,
    )
    return {"rows_bumped": n}


@router.post("/users/{user_id}/cleanup-calendar/{client_calendar_id}")
async def cleanup_calendar(
    user_id: int,
    client_calendar_id: int,
    _: User = Depends(require_admin),
) -> dict:
    """Cleanup one calendar: cancel sourced events + set projections
    targeting it to absent + clear sync token.  The next reconcile
    drains the deletes then full-syncs back."""
    db = await get_database()
    await admin_ops.cleanup_one_calendar(
        db, user_id=user_id, client_calendar_id=client_calendar_id,
    )
    return {"status": "cleanup_scheduled"}


@router.post("/users/{user_id}/cleanup-and-pause")
async def cleanup_and_pause(
    user_id: int,
    _: User = Depends(require_admin),
) -> dict:
    """Global cleanup + pause: every projection goes absent, the
    outbox drains the deletes, and sync_paused stays True until the
    admin explicitly resumes."""
    db = await get_database()
    await admin_ops.cleanup_and_pause(db, user_id=user_id)
    return {"status": "cleanup_and_pause_scheduled"}


@router.post("/users/{user_id}/resume")
async def resume_sync(
    user_id: int,
    _: User = Depends(require_admin),
) -> dict:
    """Resume a previously paused user.  No data movement; the
    periodic scheduler will pick them up on its next tick."""
    db = await get_database()
    await admin_ops.resume_sync(db, user_id=user_id)
    return {"status": "resumed"}


@router.post("/users/{user_id}/full-resync")
async def full_resync(
    user_id: int,
    _: User = Depends(require_admin),
) -> dict:
    """Wipe sync tokens for every calendar the user owns.  The next
    reconcile re-fetches everything; ledger upserts dedupe so no
    Google writes happen unless content actually changed."""
    db = await get_database()
    await admin_ops.full_resync(db, user_id=user_id)
    return {"status": "sync_tokens_cleared"}


@router.post("/users/{user_id}/disconnect-calendar/{client_calendar_id}")
async def disconnect_calendar(
    user_id: int,
    client_calendar_id: int,
    _: User = Depends(require_admin),
) -> dict:
    """Cleanup + is_active=0.  The calendar will no longer be
    ingested or written to.  Stronger than cleanup-calendar
    (which leaves the calendar active for future re-sync)."""
    db = await get_database()
    await admin_ops.disconnect_calendar(
        db, user_id=user_id, client_calendar_id=client_calendar_id,
    )
    return {"status": "disconnected"}


# ---------------------------------------------------------------------------
# Manual trigger
# ---------------------------------------------------------------------------
@router.post("/users/{user_id}/sync-now")
async def sync_now(
    user_id: int,
    source_hint: str = "all",
    _: User = Depends(require_admin),
) -> dict:
    """Enqueue a manual reconcile request.  The ledger drain job
    picks it up after the settling delay (25s)."""
    from app.ledger.triggers import enqueue_manual
    db = await get_database()
    await enqueue_manual(db, user_id=user_id, source_hint=source_hint)
    return {"status": "enqueued"}


@router.post("/users/{user_id}/reconcile-now")
async def reconcile_now(
    user_id: int,
    _: User = Depends(require_admin),
) -> dict:
    """Run one full reconciliation pass synchronously and return
    the counters.

    Equivalent to ``sync-now`` + waiting for the drain to fire,
    but without the 25-second settling delay or scheduler latency.
    Useful for tests and admin "I want this NOW" buttons.
    """
    from app.ledger.runtime import reconcile_user_by_id
    out = await reconcile_user_by_id(user_id)
    return {"status": "done", "result": out}
