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


_settings = get_settings()
limiter = Limiter(
    key_func=_get_real_ip,
    default_limits=[f"{_settings.rate_limit_per_minute}/minute"],
)
