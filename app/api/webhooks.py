"""Webhook receiver for Google Calendar push notifications."""

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Request, status

from app.config import get_settings
from app.database import get_database
from app.rate_limit import limiter, webhook_rate_key

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webhooks", tags=["webhooks"])

_wh_limit = f"{get_settings().webhook_rate_limit_per_minute}/minute"

# User ids with a webhook-triggered delayed drain already scheduled.
# enqueue_webhook already debounces the reconcile request itself, so a
# burst of webhooks for one user must collapse into a SINGLE delayed
# drain task — otherwise every POST spawns its own sleep-then-reconcile
# coroutine and a flood stacks them without bound.
_pending_webhook_drains: set[int] = set()


def _claim_webhook_drain(user_id: int) -> bool:
    """Return True when the caller should spawn a delayed drain for
    this user, False when one is already pending (the new webhook
    folds into it via the debounced reconcile request)."""
    if user_id in _pending_webhook_drains:
        return False
    _pending_webhook_drains.add(user_id)
    return True


def _release_webhook_drain(user_id: int) -> None:
    """Mark a user's delayed drain as finished so the next webhook can
    schedule a fresh one."""
    _pending_webhook_drains.discard(user_id)


@router.post("/google-calendar")
@limiter.limit(_wh_limit, key_func=webhook_rate_key)
async def receive_google_calendar_webhook(
    request: Request,
    x_goog_channel_id: str = Header(None, alias="X-Goog-Channel-ID"),
    x_goog_channel_token: str = Header(None, alias="X-Goog-Channel-Token"),
    x_goog_resource_id: str = Header(None, alias="X-Goog-Resource-ID"),
    x_goog_resource_state: str = Header(None, alias="X-Goog-Resource-State"),
    x_goog_message_number: str = Header(None, alias="X-Goog-Message-Number"),
):
    """
    Receive push notifications from Google Calendar.

    Google sends a POST request with headers indicating what changed.
    We don't receive the actual event data - we need to fetch it ourselves.
    """
    if not x_goog_channel_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing channel ID"
        )

    logger.info(
        f"Webhook received: channel={x_goog_channel_id}, "
        f"resource={x_goog_resource_id}, state={x_goog_resource_state}"
    )

    # Handle sync message (sent when webhook is first registered)
    if x_goog_resource_state == "sync":
        logger.info(f"Sync message for channel {x_goog_channel_id}")
        return {"status": "ok"}

    # Look up the channel in our database
    db = await get_database()
    cursor = await db.execute(
        """SELECT * FROM webhook_channels WHERE channel_id = ?""",
        (x_goog_channel_id,)
    )
    channel = await cursor.fetchone()

    if not channel:
        logger.warning(f"Unknown webhook channel: {x_goog_channel_id}")
        # Don't return error - Google will keep retrying
        return {"status": "ok", "message": "Unknown channel"}

    # Verify the shared secret token to confirm the request came from Google.
    # Channels registered without a token (legacy rows with empty string) are
    # accepted so that in-flight channels survive a rolling deploy; they will
    # be replaced by the normal renewal cycle within 6 days.
    import hmac
    stored_token = channel["token"] if channel["token"] else ""
    if stored_token and not hmac.compare_digest(stored_token, x_goog_channel_token or ""):
        logger.warning(f"Webhook token mismatch for channel {x_goog_channel_id}")
        return {"status": "ok"}

    # Verify resource ID matches the channel we registered.
    # If it doesn't match, ignore the notification to avoid triggering sync
    # for a potentially spoofed or stale webhook.
    if (
        x_goog_resource_id
        and channel["resource_id"]
        and x_goog_resource_id != channel["resource_id"]
    ):
        logger.warning(
            f"Webhook resource mismatch for channel {x_goog_channel_id}: "
            f"expected={channel['resource_id']} got={x_goog_resource_id}"
        )
        return {"status": "ok", "message": "Resource mismatch"}

    # Check if channel is expired.  Stored expirations are naive UTC,
    # but fold a tz-aware value to naive so the comparison against a
    # naive utcnow() cannot raise TypeError.
    if channel["expiration"]:
        expiry = datetime.fromisoformat(channel["expiration"])
        if expiry.tzinfo is not None:
            expiry = expiry.astimezone(timezone.utc).replace(tzinfo=None)
        if datetime.utcnow() > expiry:
            logger.warning(f"Expired webhook channel: {x_goog_channel_id}, cleaning up")
            # Delete expired channel from database
            await db.execute(
                "DELETE FROM webhook_channels WHERE channel_id = ?",
                (x_goog_channel_id,)
            )
            await db.commit()
            # Re-register the user's webhook channels.  The renewal
            # job only renews channels still in the table, so a
            # channel deleted here would otherwise be left without a
            # webhook until a full re-registration.  Run in the
            # background so the webhook ack stays fast.
            try:
                from app.jobs.webhook_renewal import register_webhooks_for_user
                from app.utils.tasks import create_background_task

                async def _reregister(user_id: int) -> None:
                    try:
                        await register_webhooks_for_user(user_id)
                    except Exception:
                        logger.exception(
                            "webhook re-registration failed for user %s",
                            user_id,
                        )

                create_background_task(
                    _reregister(channel["user_id"]),
                    f"webhook_reregister_user_{channel['user_id']}",
                )
            except Exception as e:
                logger.warning(
                    "could not schedule webhook re-registration: %s", e,
                )
            return {"status": "ok", "message": "Channel expired and removed"}

    # While maintenance mode is on (a DB restore is in progress) the
    # sync engine is hard-frozen — don't enqueue or drain.  The
    # webhook is still acked so Google does not retry-storm.
    from app.maintenance import in_maintenance
    if in_maintenance():
        logger.info("Webhook ignored: maintenance mode active")
        return {"status": "ok"}

    # Enqueue a debounced reconcile request and schedule an
    # immediate drain pass so latency matches the plan's 5s
    # debounce rather than the scheduler's 30s tick.
    try:
        from app.ledger.runtime import reconcile_user_by_id
        from app.ledger.triggers import WEBHOOK_DEBOUNCE, enqueue_webhook
        from app.utils.tasks import create_background_task
        if channel["calendar_type"] == "main":
            hint = "main"
        elif channel["calendar_type"] == "personal":
            hint = f"personal:{channel['client_calendar_id']}"
        else:
            hint = f"client:{channel['client_calendar_id']}"
        user_id = channel["user_id"]
        await enqueue_webhook(db, user_id=user_id, source_hint=hint)

        # Sleep out the debounce window, then drain.  Run as a
        # background task so the webhook ack is fast; the sleep gives
        # Google's eventual-consistency window time to settle before we
        # ingest.  At most one such task per user is in flight — a
        # webhook arriving while one is pending folds into the
        # debounced request above.
        if _claim_webhook_drain(user_id):
            async def _delayed_drain(uid: int) -> None:
                import asyncio
                try:
                    await asyncio.sleep(WEBHOOK_DEBOUNCE.total_seconds())
                    await reconcile_user_by_id(uid)
                except Exception:
                    logger.exception(
                        "webhook-triggered reconcile failed for user %s", uid,
                    )
                finally:
                    _release_webhook_drain(uid)

            try:
                create_background_task(
                    _delayed_drain(user_id),
                    f"webhook_drain_user_{user_id}",
                )
            except BaseException:
                # The drain task never started, so the finally-block
                # release inside it will never run.  Release the claim
                # here — otherwise this user's webhooks would never
                # schedule another drain.
                _release_webhook_drain(user_id)
                raise
        else:
            logger.debug(
                "webhook drain already pending for user %s — folded", user_id,
            )
    except Exception as e:
        logger.warning("ledger webhook enqueue/drain failed: %s", e)

    logger.info(
        "Webhook handled: calendar_type=%s calendar_id=%s",
        channel["calendar_type"], channel["client_calendar_id"],
    )
    return {"status": "ok"}


async def register_webhook_channel(
    user_id: int,
    calendar_type: str,
    calendar_id: str,
    client_calendar_id: int = None,
    access_token: str = None,
) -> dict:
    """
    Register a webhook channel with Google Calendar.

    Returns the channel info including expiration time.
    """
    import secrets
    import uuid
    from datetime import timedelta
    import httpx
    from app.config import get_settings

    settings = get_settings()
    channel_id = str(uuid.uuid4())
    channel_token = secrets.token_urlsafe(32)
    webhook_url = f"{settings.public_url}/api/webhooks/google-calendar"

    # Google webhooks expire after max 7 days, we'll set for 6 days
    expiration = datetime.utcnow() + timedelta(days=6)
    expiration_ms = int(expiration.timestamp() * 1000)

    body = {
        "id": channel_id,
        "type": "web_hook",
        "address": webhook_url,
        "expiration": str(expiration_ms),
        "token": channel_token,
    }

    # Make the watch request
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events/watch",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json=body,
        )

        if response.status_code != 200:
            logger.error(f"Failed to register webhook: {response.text}")
            raise ValueError(f"Failed to register webhook: {response.text}")

        result = response.json()

    # Store the channel in database
    db = await get_database()
    await db.execute(
        """INSERT INTO webhook_channels
           (user_id, calendar_type, client_calendar_id, channel_id, resource_id, token, expiration)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            user_id,
            calendar_type,
            client_calendar_id,
            channel_id,
            result["resourceId"],
            channel_token,
            expiration.isoformat(),
        )
    )
    await db.commit()

    logger.info(f"Registered webhook channel {channel_id} for calendar {calendar_id}")

    return {
        "channel_id": channel_id,
        "resource_id": result["resourceId"],
        "expiration": expiration.isoformat(),
    }


async def stop_webhook_channel(channel_id: str, resource_id: str, access_token: str) -> bool:
    """Stop (unregister) a webhook channel."""
    import httpx

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://www.googleapis.com/calendar/v3/channels/stop",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                json={
                    "id": channel_id,
                    "resourceId": resource_id,
                },
            )

            # 404 is OK - channel might already be stopped
            if response.status_code not in [200, 204, 404]:
                logger.warning(f"Failed to stop webhook channel: {response.text}")
                return False

        # Remove from database
        db = await get_database()
        await db.execute(
            "DELETE FROM webhook_channels WHERE channel_id = ?",
            (channel_id,)
        )
        await db.commit()

        logger.info(f"Stopped webhook channel {channel_id}")
        return True

    except Exception as e:
        logger.exception(f"Error stopping webhook channel: {e}")
        return False


async def stop_channels_for_user(
    db,
    *,
    user_id: int,
    client_calendar_id: Optional[int] = None,
) -> int:
    """Tell Google to stop every push channel for this user — or just for
    one client calendar — then remove the local rows.

    Called from the disconnect / reauth / delete paths.  Without this,
    deleting the local webhook_channels row leaves the channel live on
    Google's side, which keeps POSTing to our endpoint for the channel's
    full ~7-day TTL (the "Unknown webhook channel" storm) and also blocks
    the retention cleanup's client_calendars delete via the RESTRICT FK.

    Best-effort, mirroring the renewal job's stop pattern: a revoked or
    expired token, or a channel Google already forgot (404), is logged and
    the local row removed anyway, so the channel is never left dangling
    in our DB and the FK is cleared.  Returns the count stopped on Google.
    """
    from app.auth.google import get_valid_access_token

    bare_where = "user_id = ?"
    join_where = "wc.user_id = ?"
    params: tuple = (user_id,)
    if client_calendar_id is not None:
        bare_where += " AND client_calendar_id = ?"
        join_where += " AND wc.client_calendar_id = ?"
        params = (user_id, client_calendar_id)

    # Resolve each channel's owning account so we can fetch a token and
    # stop it on Google.  A minimal/older schema (some tests) may lack the
    # oauth_tokens/users tables — fall back to a bare query and just remove
    # the local rows, so a disconnect never errors and never leaves a row
    # dangling.
    can_resolve_token = True
    try:
        rows = await (await db.execute(
            f"""SELECT wc.channel_id, wc.resource_id, wc.calendar_type,
                       u.email AS user_email, ot.google_account_email
                  FROM webhook_channels wc
                  JOIN users u ON u.id = wc.user_id
                  LEFT JOIN client_calendars cc ON cc.id = wc.client_calendar_id
                  LEFT JOIN oauth_tokens ot ON ot.id = cc.oauth_token_id
                 WHERE {join_where}""",
            params,
        )).fetchall()
    except Exception:
        can_resolve_token = False
        rows = await (await db.execute(
            f"SELECT channel_id, resource_id, calendar_type "
            f"FROM webhook_channels WHERE {bare_where}",
            params,
        )).fetchall()

    stopped = 0
    for wc in rows:
        if can_resolve_token:
            email = (
                wc["user_email"] if wc["calendar_type"] == "main"
                else wc["google_account_email"]
            )
            try:
                if email:
                    token = await get_valid_access_token(user_id, email)
                    # stop_webhook_channel stops on Google AND deletes the row.
                    if await stop_webhook_channel(
                        wc["channel_id"], wc["resource_id"], token,
                    ):
                        stopped += 1
                        continue
            except Exception as e:
                logger.warning(
                    "could not stop webhook channel %s on Google (%s); "
                    "removing the local row anyway",
                    wc["channel_id"], e,
                )
        # Couldn't stop on Google (no/expired token, non-OK response, or a
        # schema without token tables): still drop the local row so it is
        # not left dangling.
        await db.execute(
            "DELETE FROM webhook_channels WHERE channel_id = ?",
            (wc["channel_id"],),
        )
        await db.commit()
    return stopped
