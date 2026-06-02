"""Rate limiter shared across routers."""

from fastapi import Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.config import get_settings


def _get_real_ip(request: Request) -> str:
    """Extract the client IP used to key rate limits.

    Proxy headers (``X-Real-IP`` / ``X-Forwarded-For``) are trusted
    ONLY when ``settings.trust_proxy_headers`` is set — i.e. the
    operator has confirmed a reverse proxy that overwrites them sits
    in front.  On a direct deployment any client can send those
    headers, so trusting them unconditionally would let an attacker
    mint a fresh rate-limit bucket per forged IP.  Without that
    setting we always key on the real connection IP.
    """
    if get_settings().trust_proxy_headers:
        # X-Real-IP: a trusted proxy overwrites this with the
        # connecting client's IP (not appendable by the client).
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip.strip()
        # X-Forwarded-For: client, proxy1, proxy2 — the rightmost
        # entry is the one the trusted proxy appended.
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[-1].strip()

    return get_remote_address(request)


def webhook_rate_key(request: Request) -> str:
    """Rate-limit key for the Google Calendar webhook endpoint.

    Google delivers every push notification from a small pool of Google
    IPs — and behind a reverse proxy they all arrive as a SINGLE source
    IP.  Keying on IP therefore forces every channel to share one bucket,
    so a burst (or a stale-channel storm) 429s real notifications and sync
    silently degrades to the slow scheduler poll.  Key on the push channel
    id instead: each channel gets its own bucket, so one noisy or stale
    channel can't starve the others and legitimate notifications are never
    dropped because a different channel was busy.  Falls back to the
    connection IP for malformed requests with no channel id (the handler
    rejects those with 400 anyway).
    """
    chan = request.headers.get("X-Goog-Channel-ID")
    if chan:
        return f"wh-chan:{chan}"
    return _get_real_ip(request)


_settings = get_settings()
limiter = Limiter(
    key_func=_get_real_ip,
    default_limits=[f"{_settings.rate_limit_per_minute}/minute"],
)
