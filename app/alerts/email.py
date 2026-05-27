"""Email sending functionality."""

import logging
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional

import aiosmtplib

from app.database import get_database, get_setting
from app.encryption import decrypt_value

logger = logging.getLogger(__name__)


async def get_smtp_config() -> dict:
    """Get SMTP configuration from settings."""
    config = {}

    host = await get_setting("smtp_host")
    if host:
        config["host"] = host.get("value_plain")

    port = await get_setting("smtp_port")
    if port:
        config["port"] = int(port.get("value_plain") or 587)
    else:
        config["port"] = 587

    username = await get_setting("smtp_username")
    if username:
        config["username"] = username.get("value_plain")

    password = await get_setting("smtp_password")
    if password and password.get("value_encrypted"):
        config["password"] = decrypt_value(password["value_encrypted"])

    from_addr = await get_setting("smtp_from_address")
    if from_addr:
        config["from_address"] = from_addr.get("value_plain")

    return config


async def send_email(
    to_email: str,
    subject: str,
    body: str,
    html_body: Optional[str] = None,
) -> None:
    """
    Send an email.

    Args:
        to_email: Recipient email address
        subject: Email subject
        body: Plain text body
        html_body: Optional HTML body
    """
    config = await get_smtp_config()

    if not config.get("host"):
        logger.warning("SMTP not configured, cannot send email")
        raise ValueError("SMTP not configured")

    # Create message
    if html_body:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(body, "plain"))
        msg.attach(MIMEText(html_body, "html"))
    else:
        msg = MIMEText(body, "plain")

    msg["Subject"] = subject
    msg["From"] = config.get("from_address", config.get("username"))
    msg["To"] = to_email

    # Send email
    try:
        await aiosmtplib.send(
            msg,
            hostname=config["host"],
            port=config["port"],
            username=config.get("username"),
            password=config.get("password"),
            start_tls=True,
        )
        logger.info(f"Email sent to {to_email}: {subject}")

    except Exception as e:
        logger.error(f"Failed to send email to {to_email}: {e}")
        raise


async def queue_alert(
    alert_type: str,
    user_id: Optional[int] = None,
    calendar_id: Optional[int] = None,
    details: str = "",
) -> None:
    """
    Queue an alert for sending.

    Handles deduplication (same alert type for same calendar within 1 hour).
    """
    db = await get_database()

    # Check if alerts are enabled
    enabled = await get_setting("alerts_enabled")
    if not enabled or enabled.get("value_plain") != "true":
        logger.debug("Alerts are disabled")
        return

    # Check for duplicate (same alert type for same calendar within 1 hour)
    cursor = await db.execute(
        """SELECT id FROM alert_queue
           WHERE alert_type = ? AND datetime(created_at) > datetime('now', '-1 hour')
           AND (? IS NULL OR recipient_email IN (
               SELECT email FROM users WHERE id = ?
           ))""",
        (alert_type, user_id, user_id)
    )
    existing = await cursor.fetchone()

    if existing:
        logger.debug(f"Skipping duplicate alert: {alert_type}")
        return

    # Get recipients
    recipients = []

    # Add affected user
    if user_id:
        cursor = await db.execute("SELECT email FROM users WHERE id = ?", (user_id,))
        user = await cursor.fetchone()
        if user:
            recipients.append(user["email"])

    # Add admin emails
    admin_emails = await get_setting("alert_emails")
    if admin_emails and admin_emails.get("value_plain"):
        for email in admin_emails["value_plain"].split(","):
            email = email.strip()
            if email and email not in recipients:
                recipients.append(email)

    if not recipients:
        logger.warning("No recipients for alert")
        return

    # Generate email content
    subject, body = generate_alert_content(alert_type, details, calendar_id)

    # Queue for each recipient
    for recipient in recipients:
        await db.execute(
            """INSERT INTO alert_queue (alert_type, recipient_email, subject, body)
               VALUES (?, ?, ?, ?)""",
            (alert_type, recipient, subject, body)
        )

    await db.commit()
    logger.info(f"Queued {alert_type} alert for {len(recipients)} recipients")


async def queue_placement_disconnected_alert(
    *,
    user_id: int,
    subscription_id: int,
    feed_name: str,
    feed_url: str,
    placement_target_name: str,
) -> None:
    """Raise a webcal_placement_disconnected alert (see webcal.md).

    Bypasses ``queue_alert``'s per-type dedup so disconnecting one
    client that holds placement for multiple webcal subscriptions
    still produces one email per affected subscription (each naming
    its own feed).  Dedup key is ``(alert_type, subscription_id,
    user)`` within the standard 1-hour window — a second disconnect
    of the SAME subscription's placement within an hour stays
    silent, but DIFFERENT subscriptions never collide.
    """
    db = await get_database()

    enabled = await get_setting("alerts_enabled")
    if not enabled or enabled.get("value_plain") != "true":
        logger.debug("Alerts are disabled")
        return

    # Per-subscription dedup: bake the subscription id into the
    # alert_type column so SQL-level dedup naturally distinguishes
    # subscriptions.  The user-facing subject/body stay unaware of
    # this encoding.
    alert_type_keyed = f"webcal_placement_disconnected:{subscription_id}"

    cursor = await db.execute(
        """SELECT id FROM alert_queue
           WHERE alert_type = ? AND datetime(created_at) > datetime('now', '-1 hour')
           AND recipient_email IN (
               SELECT email FROM users WHERE id = ?
           )""",
        (alert_type_keyed, user_id),
    )
    if await cursor.fetchone():
        logger.debug(
            "Skipping duplicate placement-disconnected alert for sub %s",
            subscription_id,
        )
        return

    cursor = await db.execute("SELECT email, display_name FROM users WHERE id = ?", (user_id,))
    user = await cursor.fetchone()
    if not user:
        logger.warning("No user %s for placement-disconnected alert", user_id)
        return

    recipients = [user["email"]]
    admin_emails = await get_setting("alert_emails")
    if admin_emails and admin_emails.get("value_plain"):
        for email in admin_emails["value_plain"].split(","):
            email = email.strip()
            if email and email not in recipients:
                recipients.append(email)

    subject, body = _placement_disconnected_email(
        first_name=(user["display_name"] or user["email"]).split()[0].split("@")[0],
        feed_name=feed_name or "your WebCal feed",
        feed_url=feed_url,
        placement_target_name=placement_target_name or "the disconnected calendar",
    )

    for recipient in recipients:
        await db.execute(
            """INSERT INTO alert_queue (alert_type, recipient_email, subject, body)
               VALUES (?, ?, ?, ?)""",
            (alert_type_keyed, recipient, subject, body),
        )
    await db.commit()
    logger.info(
        "Queued placement-disconnected alert for sub %s to %d recipients",
        subscription_id, len(recipients),
    )


def _placement_disconnected_email(
    *,
    first_name: str,
    feed_name: str,
    feed_url: str,
    placement_target_name: str,
) -> tuple[str, str]:
    """Concrete email copy for the placement-disconnected alert.

    Matches the template in webcal.md §Alerts.  Kept here (not in
    generate_alert_content) so the spec's user-facing copy and the
    deployed copy can be diffed against each other directly.
    """
    subject = f'BusyBridge — your "{feed_name}" WebCal needs a new home'
    body = (
        f"Hi {first_name},\n"
        "\n"
        f'You disconnected the "{placement_target_name}" calendar from BusyBridge.\n'
        "\n"
        "That calendar was the placement target for one of your WebCal feeds:\n"
        "\n"
        f"  Feed:        {feed_name}\n"
        f"  URL:         {feed_url}\n"
        f"  Was placed:  {placement_target_name} (now disconnected)\n"
        "\n"
        "What this means right now:\n"
        f"  • Events from {feed_name} still appear on your main calendar.\n"
        f"  • Events no longer appear on {placement_target_name}.\n"
        "  • Other client calendars still receive Busy blocks as usual.\n"
        "\n"
        "What to do:\n"
        f"  Open BusyBridge → WebCal subscriptions → {feed_name}, and either:\n"
        "    • pick a different client calendar as the placement target, or\n"
        '    • switch the placement to "Main calendar" only.\n'
        "\n"
        f"Until you do, the {feed_name} feed will keep working on your main "
        "calendar — just without a client-side full copy.\n"
        "\n"
        "— BusyBridge\n"
    )
    return subject, body


def generate_alert_content(
    alert_type: str,
    details: str,
    calendar_id: Optional[int] = None,
) -> tuple[str, str]:
    """Generate email subject and body for an alert."""
    from app.config import get_settings
    settings = get_settings()

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    subjects = {
        "token_revoked": "Calendar Sync - Authentication Required",
        "calendar_inaccessible": "Calendar Sync - Calendar Access Issue",
        "sync_failures": "Calendar Sync - Sync Failures Detected",
        "webhook_registration_failed": "Calendar Sync - Webhook Issue",
        "system_error": "Calendar Sync - System Error",
        "integrity_issues": "Calendar Sync - Integrity Issues Detected",
        "webcal_placement_disconnected": "BusyBridge - WebCal placement target disconnected",
    }

    subject = subjects.get(alert_type, f"Calendar Sync - {alert_type}")

    body = f"""Calendar Sync Engine Alert

Alert Type: {alert_type}
Time: {timestamp}
"""

    if calendar_id:
        body += f"Calendar ID: {calendar_id}\n"

    body += f"""
Details:
{details}

---
Manage your calendar sync settings: {settings.public_url}/app/settings
"""

    return subject, body


async def send_test_email_to(recipient: str) -> bool:
    """Send a test email to verify configuration."""
    try:
        await send_email(
            to_email=recipient,
            subject="Calendar Sync - Test Email",
            body="This is a test email from Calendar Sync Engine.\n\nIf you received this, your email configuration is working correctly.",
        )
        return True
    except Exception as e:
        logger.error(f"Test email failed: {e}")
        return False
