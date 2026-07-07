"""Webcal/ICS subscription management API endpoints."""

import json
import logging
from datetime import datetime
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, model_validator

from app.auth.session import get_current_user, User
from app.database import get_database

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webcal-subscriptions", tags=["webcal-subscriptions"])


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


# placement_target_status values, surfaced to the dashboard so it can
# render the "Placement target disconnected" badge without computing the
# state client-side.
PLACEMENT_STATUS_NOT_APPLICABLE = "not_applicable"  # placement_kind == 'main'
PLACEMENT_STATUS_ACTIVE = "active"                  # client target alive
PLACEMENT_STATUS_DISCONNECTED = "disconnected"      # client target gone or inactive

_VALID_PLACEMENT_KINDS = ("main", "client")


class WebcalSubscriptionResponse(BaseModel):
    id: int
    url: str
    display_prefix: str
    is_active: bool = True
    last_poll_at: Optional[str] = None
    last_success_at: Optional[str] = None
    sync_status: str = "unknown"
    consecutive_failures: int = 0
    last_error: Optional[str] = None
    event_count: int = 0
    # Placement fields — see webcal.md §API Changes.
    placement_kind: str = "main"
    placement_client_calendar_id: Optional[int] = None
    placement_client_display_name: Optional[str] = None
    placement_target_status: str = PLACEMENT_STATUS_NOT_APPLICABLE


def _check_placement_pair(kind: Optional[str], target_id: Optional[int]) -> None:
    """Cross-field shape check for any (kind, target_id) tuple.

    Pydantic enforces type; this enforces the "main → no target / client
    → target required" rule before we ever touch the DB.  DB-level
    existence/ownership/active checks live in
    :func:`_validate_placement_target` below.
    """
    if kind is not None and kind not in _VALID_PLACEMENT_KINDS:
        raise ValueError(
            f"placement_kind must be one of {_VALID_PLACEMENT_KINDS}, got {kind!r}"
        )
    # An orphan target_id (no kind sent) would otherwise slip through
    # to the handler as new_kind=None and corrupt the row.  Block it
    # here at the wire boundary.
    if kind is None and target_id is not None:
        raise ValueError(
            "placement_kind is required when placement_client_calendar_id is provided"
        )
    if kind == "main" and target_id is not None:
        raise ValueError(
            "placement_client_calendar_id must be null when placement_kind = 'main'"
        )
    if kind == "client" and target_id is None:
        raise ValueError(
            "placement_client_calendar_id is required when placement_kind = 'client'"
        )


class CreateWebcalRequest(BaseModel):
    url: str
    display_prefix: str = ""
    placement_kind: str = "main"
    placement_client_calendar_id: Optional[int] = None

    @model_validator(mode="after")
    def _validate_placement(self) -> "CreateWebcalRequest":
        _check_placement_pair(self.placement_kind, self.placement_client_calendar_id)
        return self


class UpdateWebcalRequest(BaseModel):
    # All optional on PATCH — a request sending only display_prefix
    # must leave placement untouched, and vice versa.
    display_prefix: Optional[str] = None
    placement_kind: Optional[str] = None
    placement_client_calendar_id: Optional[int] = None

    @model_validator(mode="after")
    def _validate_placement(self) -> "UpdateWebcalRequest":
        # We only enforce the pair-shape when the caller is actually
        # changing placement.  Sending placement_kind alone is enough
        # to enter the "validate the pair" path because that's the
        # only signal that placement was touched.
        if self.placement_kind is None and self.placement_client_calendar_id is None:
            return self
        _check_placement_pair(
            self.placement_kind, self.placement_client_calendar_id,
        )
        return self


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _validate_placement_target(
    db,
    *,
    user_id: int,
    placement_kind: str,
    placement_client_calendar_id: Optional[int],
) -> Optional[str]:
    """DB-level validation: target row exists, is active, belongs to user.

    Returns the placement client's ``display_name`` (so the caller can
    snapshot it into ``placement_client_display_name_cache``), or
    ``None`` for ``placement_kind='main'``.  Raises ``HTTPException`` on
    any failure so the API path returns a proper 400.
    """
    if placement_kind == "main":
        return None
    # placement_kind == "client" — pair check already passed.
    cursor = await db.execute(
        """SELECT id, display_name, is_active
             FROM client_calendars
            WHERE id = ? AND user_id = ?""",
        (placement_client_calendar_id, user_id),
    )
    target = await cursor.fetchone()
    if target is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="placement_client_calendar_id does not exist or does not belong to you",
        )
    if not target["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="placement_client_calendar_id refers to a disconnected calendar",
        )
    return target["display_name"]


def _derive_placement_status(
    placement_kind: str,
    placement_client_calendar_id: Optional[int],
    placement_client_is_active: Optional[int],
) -> str:
    """Convert raw DB state into the dashboard-facing status enum.

    ``placement_client_is_active`` is ``None`` when the row is gone
    entirely (FK SET NULL fired, or the LEFT JOIN found nothing).
    """
    if placement_kind != "client":
        return PLACEMENT_STATUS_NOT_APPLICABLE
    if placement_client_calendar_id is None:
        return PLACEMENT_STATUS_DISCONNECTED
    if not placement_client_is_active:
        return PLACEMENT_STATUS_DISCONNECTED
    return PLACEMENT_STATUS_ACTIVE


def _sync_log_details(payload: dict) -> str:
    return json.dumps(payload, default=str)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("", response_model=list[WebcalSubscriptionResponse])
async def list_webcal_subscriptions(user: User = Depends(get_current_user)):
    """List webcal subscriptions for current user."""
    db = await get_database()

    # LEFT JOIN client_calendars on placement_client_calendar_id so the
    # response carries the live placement client display_name and
    # is_active flag — both required to derive placement_target_status.
    # The join is LEFT because main-placed subscriptions have a NULL
    # target id, and stale placements may point at a now-deleted row
    # (ON DELETE SET NULL leaves the column NULL — caught by the same
    # branch).
    cursor = await db.execute(
        """SELECT ws.*,
                  cc.display_name AS placement_client_display_name,
                  cc.is_active    AS placement_client_is_active
             FROM webcal_subscriptions ws
        LEFT JOIN client_calendars cc
               ON cc.id = ws.placement_client_calendar_id
            WHERE ws.user_id = ? AND ws.is_active = TRUE
         ORDER BY ws.created_at DESC""",
        (user.id,),
    )
    rows = await cursor.fetchall()

    # Event counts come from the ledger.
    from app.ledger.facade import count_active_events_per_webcal_subscription
    counts = await count_active_events_per_webcal_subscription(
        db, user_id=user.id,
    )

    results = []
    for row in rows:
        sync_status = "ok"
        if row["consecutive_failures"] and row["consecutive_failures"] >= 5:
            sync_status = "error"
        elif row["consecutive_failures"] and row["consecutive_failures"] >= 1:
            sync_status = "warning"
        elif not row["last_success_at"]:
            sync_status = "pending"

        placement_kind = row["placement_kind"] or "main"
        placement_target_id = row["placement_client_calendar_id"]
        placement_status = _derive_placement_status(
            placement_kind,
            placement_target_id,
            row["placement_client_is_active"],
        )
        # If the live client row is gone or inactive, the dashboard
        # still needs *some* name to render in the badge — fall back to
        # the snapshot captured when placement was set.
        display_name = row["placement_client_display_name"]
        if placement_kind == "client" and display_name is None:
            display_name = row["placement_client_display_name_cache"]

        results.append(WebcalSubscriptionResponse(
            id=row["id"],
            url=row["url"],
            display_prefix=row["display_prefix"] or "",
            is_active=bool(row["is_active"]),
            last_poll_at=row["last_poll_at"],
            last_success_at=row["last_success_at"],
            sync_status=sync_status,
            consecutive_failures=row["consecutive_failures"] or 0,
            last_error=row["last_error"],
            event_count=counts.get(int(row["id"]), 0),
            placement_kind=placement_kind,
            placement_client_calendar_id=placement_target_id,
            placement_client_display_name=display_name,
            placement_target_status=placement_status,
        ))

    return results


@router.post("", response_model=WebcalSubscriptionResponse)
async def create_webcal_subscription(
    request: CreateWebcalRequest,
    user: User = Depends(get_current_user),
):
    """Add a new webcal subscription."""
    db = await get_database()

    # Normalize URL
    url = request.url.strip()
    if url.startswith("webcal://"):
        url = "https://" + url[len("webcal://"):]

    if not url.startswith(("https://", "http://")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="URL must start with https://, http://, or webcal://",
        )

    # Validate placement target against the DB (existence / ownership /
    # active).  Pydantic already enforced the pair-shape.
    cached_display_name = await _validate_placement_target(
        db,
        user_id=user.id,
        placement_kind=request.placement_kind,
        placement_client_calendar_id=request.placement_client_calendar_id,
    )

    # Look up any existing row for this (user, url). The unique
    # index doesn't filter on is_active, so a soft-deleted row would
    # crash the INSERT below — treat it as a reactivation instead.
    cursor = await db.execute(
        "SELECT id, is_active FROM webcal_subscriptions WHERE user_id = ? AND url = ?",
        (user.id, url),
    )
    existing = await cursor.fetchone()
    if existing and existing["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="This URL is already subscribed",
        )

    # SSRF check + validate by trying to fetch
    from app.utils.ics_fetch import fetch_ics_feed, validate_url_for_ssrf
    try:
        validate_url_for_ssrf(url)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="URL must point to a public internet address",
        )
    try:
        content, etag = await fetch_ics_feed(url)
    except ValueError:
        # SSRF blocked (e.g. redirect to internal IP)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="URL must point to a public internet address",
        )
    except httpx.HTTPError as e:
        logger.warning("webcal feed fetch failed for %s: %s", url, e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not fetch the feed from this URL.",
        )
    except Exception as e:
        logger.warning("webcal feed parse failed for %s: %s", url, e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The URL did not return a valid ICS calendar feed.",
        )

    # Checked outside the try: raising this 400 inside it would be
    # swallowed by the generic handler above and re-labelled as a
    # parse failure.
    if content is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not fetch ICS feed from this URL",
        )

    if existing:
        # Reactivate the soft-deleted row with the new settings.
        # Clear poll state so the next reconcile treats this as a
        # fresh subscription (sync_status='pending' until first ok).
        sub_id = int(existing["id"])
        await db.execute(
            """UPDATE webcal_subscriptions
                  SET is_active = TRUE,
                      display_prefix = ?,
                      placement_kind = ?,
                      placement_client_calendar_id = ?,
                      placement_client_display_name_cache = ?,
                      last_poll_at = NULL,
                      last_etag = NULL,
                      last_success_at = NULL,
                      consecutive_failures = 0,
                      last_error = NULL,
                      updated_at = ?
                WHERE id = ?""",
            (
                request.display_prefix.strip(),
                request.placement_kind,
                request.placement_client_calendar_id,
                cached_display_name,
                datetime.utcnow().isoformat(),
                sub_id,
            ),
        )
    else:
        cursor = await db.execute(
            """INSERT INTO webcal_subscriptions
                  (user_id, url, display_prefix,
                   placement_kind, placement_client_calendar_id,
                   placement_client_display_name_cache)
               VALUES (?, ?, ?, ?, ?, ?)
               RETURNING id""",
            (
                user.id,
                url,
                request.display_prefix.strip(),
                request.placement_kind,
                request.placement_client_calendar_id,
                cached_display_name,
            ),
        )
        row = await cursor.fetchone()
        sub_id = row["id"]
    await db.commit()

    # Log
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'connect_webcal', 'success', ?)""",
        (
            user.id,
            _sync_log_details({
                "url": url,
                "prefix": request.display_prefix,
                "placement_kind": request.placement_kind,
                "placement_client_calendar_id": request.placement_client_calendar_id,
            }),
        ),
    )
    await db.commit()

    # Trigger initial sync via the ledger queue.
    from app.ledger.triggers import enqueue_manual
    await enqueue_manual(
        db, user_id=user.id, source_hint=f"webcal:{sub_id}",
    )

    placement_status = PLACEMENT_STATUS_NOT_APPLICABLE
    if request.placement_kind == "client":
        placement_status = PLACEMENT_STATUS_ACTIVE  # we just validated it

    return WebcalSubscriptionResponse(
        id=sub_id,
        url=url,
        display_prefix=request.display_prefix.strip(),
        is_active=True,
        sync_status="pending",
        placement_kind=request.placement_kind,
        placement_client_calendar_id=request.placement_client_calendar_id,
        placement_client_display_name=cached_display_name,
        placement_target_status=placement_status,
    )


@router.delete("/{subscription_id}")
async def delete_webcal_subscription(
    subscription_id: int,
    user: User = Depends(get_current_user),
):
    """Remove a webcal subscription and clean up synced events."""
    db = await get_database()

    cursor = await db.execute(
        "SELECT * FROM webcal_subscriptions WHERE id = ? AND user_id = ? AND is_active = TRUE",
        (subscription_id, user.id),
    )
    sub = await cursor.fetchone()
    if not sub:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription not found",
        )

    # Cancel every ledger row sourced from this subscription so
    # the next reconcile drains the deletes.
    now_iso = datetime.utcnow().isoformat()
    cancelled = await (await db.execute(
        """UPDATE ledger_events
              SET status = 'cancelled',
                  version = version + 1,
                  cancelled_at = ?, updated_at = ?
            WHERE user_id = ? AND source_type = 'webcal'
              AND source_calendar_id = ? AND status = 'active'
           RETURNING id""",
        (now_iso, now_iso, user.id, subscription_id),
    )).fetchall()
    await db.execute(
        "UPDATE webcal_subscriptions SET is_active = FALSE, updated_at = ? WHERE id = ?",
        (now_iso, subscription_id),
    )
    await db.commit()
    # Record the cancelled rows as affected so the reconciler actually
    # replans them — the trigger's source_hint is not persisted, so
    # enqueue_manual alone leaves _consume_affected_ledger_ids empty
    # and the planner would never flip these projections to absent.
    from app.ledger.admin_ops import _append_affected
    from app.ledger.triggers import enqueue_manual
    await _append_affected(
        db, user_id=user.id, ledger_ids=[int(r["id"]) for r in cancelled],
    )
    await enqueue_manual(db, user_id=user.id, source_hint="all")

    # Log
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'disconnect_webcal', 'success', ?)""",
        (user.id, _sync_log_details({"url": sub["url"]})),
    )
    await db.commit()

    return {"status": "ok", "message": "Webcal subscription removed"}


@router.post("/{subscription_id}/sync")
async def trigger_webcal_sync(
    subscription_id: int,
    user: User = Depends(get_current_user),
):
    """Trigger manual sync for a webcal subscription."""
    db = await get_database()

    cursor = await db.execute(
        "SELECT * FROM webcal_subscriptions WHERE id = ? AND user_id = ? AND is_active = TRUE",
        (subscription_id, user.id),
    )
    if not await cursor.fetchone():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription not found",
        )

    from app.ledger.triggers import enqueue_manual
    await enqueue_manual(
        db, user_id=user.id, source_hint=f"webcal:{subscription_id}",
    )
    return {"status": "ok", "message": "Sync triggered"}


@router.patch("/{subscription_id}")
async def update_webcal_subscription(
    subscription_id: int,
    request: UpdateWebcalRequest,
    user: User = Depends(get_current_user),
):
    """Update a webcal subscription's prefix and/or placement.

    See webcal.md §Placement Changes.  The placement transition runs as
    a single transaction: validate → persist → cache → append affected
    → enqueue_manual → sync_log.  A no-op placement (caller sent the
    same values) skips every side effect.
    """
    db = await get_database()

    cursor = await db.execute(
        """SELECT id, display_prefix,
                  placement_kind,
                  placement_client_calendar_id,
                  placement_client_display_name_cache
             FROM webcal_subscriptions
            WHERE id = ? AND user_id = ? AND is_active = TRUE""",
        (subscription_id, user.id),
    )
    sub = await cursor.fetchone()
    if not sub:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription not found",
        )

    # Are we touching placement at all?
    placement_touched = (
        request.placement_kind is not None
        or request.placement_client_calendar_id is not None
    )

    # Decide the post-update placement values (None when not touching).
    new_kind: Optional[str] = None
    new_target_id: Optional[int] = None
    new_cache: Optional[str] = None
    placement_actually_changed = False

    if placement_touched:
        new_kind = request.placement_kind
        new_target_id = request.placement_client_calendar_id

        # Validate against the DB before deciding no-op vs change, so a
        # bogus payload always 400s even when the values happen to
        # match the row.
        new_cache = await _validate_placement_target(
            db,
            user_id=user.id,
            placement_kind=new_kind,
            placement_client_calendar_id=new_target_id,
        )

        # No-op idempotency (spec §Placement Changes step 0): if
        # nothing would actually change, skip the entire replan path.
        # Note: we deliberately don't compare the cache — it's a
        # derived snapshot, not a user-supplied value.
        current_kind = sub["placement_kind"] or "main"
        current_target = sub["placement_client_calendar_id"]
        placement_actually_changed = (
            new_kind != current_kind or new_target_id != current_target
        )

    # display_prefix changes are independent of placement.
    new_prefix = (
        request.display_prefix.strip()
        if request.display_prefix is not None
        else None
    )
    prefix_changed = (
        new_prefix is not None and new_prefix != (sub["display_prefix"] or "")
    )

    now_iso = datetime.utcnow().isoformat()
    something_changed = False

    if placement_actually_changed:
        # Single transaction: persist placement, refresh the display
        # name cache, append every active ledger row from this
        # subscription, write sync_log.  If ANY step raises, the
        # whole transaction rolls back so placement and the replan
        # queue (`affected_ledger_events`) never disagree (spec
        # §Placement Changes).
        #
        # enqueue_manual is deliberately OUTSIDE the BEGIN: it calls
        # _upsert_request which COMMITs internally and so cannot run
        # inside another transaction.  This means a failing wake-up
        # signal can leave placement committed without an immediate
        # reconcile request — but the periodic reconciler picks up
        # `affected_ledger_events` rows regardless, so worst case is
        # a one-cycle delay, never a state inconsistency.
        try:
            await db.execute("BEGIN")
            await db.execute(
                """UPDATE webcal_subscriptions
                      SET placement_kind = ?,
                          placement_client_calendar_id = ?,
                          placement_client_display_name_cache = ?,
                          updated_at = ?
                    WHERE id = ?""",
                (new_kind, new_target_id, new_cache, now_iso, subscription_id),
            )
            affected = await (await db.execute(
                """SELECT id FROM ledger_events
                    WHERE user_id = ? AND source_type = 'webcal'
                      AND source_calendar_id = ? AND status = 'active'""",
                (user.id, subscription_id),
            )).fetchall()
            ledger_ids = [int(r["id"]) for r in affected]
            from app.ledger.triggers import record_affected_events
            await record_affected_events(
                db, user_id=user.id, ledger_event_ids=ledger_ids,
            )
            await db.execute(
                """INSERT INTO sync_log (user_id, action, status, details)
                   VALUES (?, 'change_placement', 'success', ?)""",
                (
                    user.id,
                    _sync_log_details({
                        "subscription_id": subscription_id,
                        "old_kind": sub["placement_kind"] or "main",
                        "old_target_id": sub["placement_client_calendar_id"],
                        "new_kind": new_kind,
                        "new_target_id": new_target_id,
                        "affected_ledger_count": len(ledger_ids),
                    }),
                ),
            )
            await db.execute("COMMIT")
        except BaseException:
            await db.execute("ROLLBACK")
            raise
        # Wake-up signal: best-effort, AFTER the placement is durable.
        from app.ledger.triggers import enqueue_manual
        await enqueue_manual(
            db, user_id=user.id, source_hint=f"webcal:{subscription_id}",
        )
        something_changed = True

    if prefix_changed:
        # display_prefix change must replan every active ledger row
        # from this subscription so the Source: footer is re-rendered
        # with the new prefix.  Re-fetching the feed alone is not
        # enough: display_prefix is NOT part of the ICS content_hash
        # (see app/ledger/ingest/client.py:_content_hash), so the
        # ingest would skip unchanged rows and the planner would
        # never re-evaluate.  Same atomic pattern as placement
        # changes: persist + append affected in one transaction;
        # wake-up signal is best-effort outside.
        try:
            await db.execute("BEGIN")
            await db.execute(
                """UPDATE webcal_subscriptions
                      SET display_prefix = ?, last_etag = NULL, updated_at = ?
                    WHERE id = ?""",
                (new_prefix, now_iso, subscription_id),
            )
            affected = await (await db.execute(
                """SELECT id FROM ledger_events
                    WHERE user_id = ? AND source_type = 'webcal'
                      AND source_calendar_id = ? AND status = 'active'""",
                (user.id, subscription_id),
            )).fetchall()
            ledger_ids = [int(r["id"]) for r in affected]
            from app.ledger.triggers import record_affected_events
            await record_affected_events(
                db, user_id=user.id, ledger_event_ids=ledger_ids,
            )
            await db.execute("COMMIT")
        except BaseException:
            await db.execute("ROLLBACK")
            raise
        from app.ledger.triggers import enqueue_manual
        await enqueue_manual(
            db, user_id=user.id, source_hint=f"webcal:{subscription_id}",
        )
        something_changed = True

    if not something_changed:
        # No-op — the spec explicitly wants this to succeed silently.
        return {"status": "ok", "message": "No changes"}

    return {"status": "ok", "message": "Updated"}
