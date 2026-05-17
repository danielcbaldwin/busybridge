"""SSRF-safe ICS feed fetching.

Lifted out of the legacy ``app/sync/ics_parser.py`` so the API
layer and the new ledger webcal-ingest hook can both use it
without dragging in the rest of the legacy sync engine.

The fetch is hardened against SSRF on three fronts:

* every hostname is resolved and every resolved address checked
  against private/reserved ranges before a connection is made;
* redirects are followed manually, one hop at a time, so an
  intermediate hop cannot reach an internal service unchecked;
* the connection is pinned to the exact IP that was validated —
  httpx never re-resolves the name — which closes the DNS-rebinding
  window between the check and the connect.
"""

from __future__ import annotations

import ipaddress
import logging
import socket
from typing import Optional
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

# Hard cap on a fetched ICS body.  A feed is plain text; anything past
# this is almost certainly hostile (or broken) and reading it whole
# into memory would be a DoS.  Enforced by streaming, so an oversized
# body is abandoned mid-download rather than buffered.
_MAX_ICS_BYTES = 10 * 1024 * 1024  # 10 MiB

# Cap on redirects followed before giving up.  Each hop is validated
# individually, so this is just a loop guard against a redirect cycle.
_MAX_REDIRECTS = 5

# Explicit blocklist for defence-in-depth (covers cloud metadata,
# RFC 1918, carrier-grade NAT, benchmarking, documentation, and
# broadcast ranges that some older Python ipaddress builds may
# not flag via is_private/is_reserved).
_BLOCKED_NETWORKS = [
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.0.0.0/24"),
    ipaddress.ip_network("192.0.2.0/24"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("198.18.0.0/15"),
    ipaddress.ip_network("198.51.100.0/24"),
    ipaddress.ip_network("203.0.113.0/24"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("240.0.0.0/4"),
    ipaddress.ip_network("255.255.255.255/32"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]


def _is_ip_blocked(ip_str: str) -> bool:
    """True if ``ip_str`` is a private/reserved IP.

    ``ip_str`` must be a literal IP address — callers resolve
    hostnames themselves and pass the resulting addresses here.
    """
    addr = ipaddress.ip_address(ip_str)  # raises ValueError on a non-IP
    if (
        addr.is_private
        or addr.is_reserved
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_multicast
        or addr.is_unspecified
    ):
        return True
    for network in _BLOCKED_NETWORKS:
        if addr in network:
            return True
    return False


def _resolve_and_validate(hostname: str) -> str:
    """Resolve ``hostname`` (or accept an IP literal) and return one
    safe IP address to connect to.

    Raises ``ValueError`` if the host is unresolvable or *any* address
    it maps to is private/reserved.  Rejecting on any blocked address
    — not just the one we would have picked — defeats a resolver that
    returns a mix of public and internal records.  The returned IP is
    meant to be connected to directly so the name is never resolved a
    second time (DNS-rebinding defence).
    """
    # A literal IP needs no DNS lookup.
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        pass
    else:
        if _is_ip_blocked(hostname):
            raise ValueError("URL points to a blocked address")
        return hostname

    try:
        addrinfos = socket.getaddrinfo(
            hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM,
        )
    except socket.gaierror:
        raise ValueError(f"Cannot resolve hostname: {hostname}")
    if not addrinfos:
        raise ValueError(f"Cannot resolve hostname: {hostname}")

    safe_ip: Optional[str] = None
    for _family, _type, _proto, _canonname, sockaddr in addrinfos:
        ip = sockaddr[0]
        if _is_ip_blocked(ip):
            raise ValueError("URL resolves to a blocked address")
        if safe_ip is None:
            safe_ip = ip
    assert safe_ip is not None  # addrinfos was non-empty
    return safe_ip


def validate_url_for_ssrf(url: str) -> None:
    """Raise ``ValueError`` if ``url`` targets a private/reserved
    network.  Used by the API layer at webcal-subscription creation."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"unsupported URL scheme: {parsed.scheme!r}")
    if not parsed.hostname:
        raise ValueError("URL has no hostname")
    _resolve_and_validate(parsed.hostname)


def _host_header(hostname: str, port: int, scheme: str) -> str:
    """The Host header value — hostname alone, plus the port when it
    is not the scheme default."""
    default = 443 if scheme == "https" else 80
    return hostname if port == default else f"{hostname}:{port}"


async def fetch_ics_feed(
    url: str,
    etag: Optional[str] = None,
    timeout: float = 30.0,
) -> tuple[Optional[str], Optional[str]]:
    """SSRF-safe ICS fetch returning ``(content, new_etag)`` or
    ``(None, None)`` on 304 Not Modified.

    Redirects are followed manually, one hop at a time.  Each hop's
    hostname is resolved and validated, then the connection is pinned
    to the validated IP (the request is issued against the IP with the
    ``Host`` header and TLS SNI set to the original hostname) so httpx
    cannot re-resolve the name to a rebinding target.
    """
    if url.startswith("webcal://"):
        url = "https://" + url[len("webcal://"):]

    base_headers = {}
    if etag:
        base_headers["If-None-Match"] = etag

    current_url = url
    async with httpx.AsyncClient(
        follow_redirects=False, timeout=timeout,
    ) as client:
        for _hop in range(_MAX_REDIRECTS + 1):
            parsed = urlparse(current_url)
            if parsed.scheme not in ("http", "https"):
                raise ValueError(
                    f"unsupported URL scheme: {parsed.scheme!r}"
                )
            hostname = parsed.hostname
            if not hostname:
                raise ValueError("URL has no hostname")

            # Resolve + validate, then connect to that exact IP.
            safe_ip = _resolve_and_validate(hostname)
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            ip_host = f"[{safe_ip}]" if ":" in safe_ip else safe_ip
            pinned_url = f"{parsed.scheme}://{ip_host}:{port}{parsed.path or '/'}"
            if parsed.query:
                pinned_url += f"?{parsed.query}"

            request = client.build_request(
                "GET", pinned_url,
                headers={
                    **base_headers,
                    "Host": _host_header(hostname, port, parsed.scheme),
                },
            )
            # TLS SNI / certificate verification still use the real
            # hostname even though the socket connects to the IP.
            request.extensions["sni_hostname"] = hostname

            response = await client.send(request, stream=True)
            try:
                if response.is_redirect:
                    location = response.headers.get("Location")
                    if not location:
                        raise ValueError(
                            "redirect response carried no Location header"
                        )
                    # Resolve a relative redirect against the current
                    # (hostname-based) URL; the next iteration validates
                    # and pins the result.
                    current_url = str(httpx.URL(current_url).join(location))
                    continue
                if response.status_code == 304:
                    return None, None
                response.raise_for_status()
                # Stream with a running byte cap so an oversized (or
                # endless) body is abandoned instead of buffered whole.
                chunks: list[bytes] = []
                total = 0
                async for chunk in response.aiter_bytes():
                    total += len(chunk)
                    if total > _MAX_ICS_BYTES:
                        raise ValueError(
                            f"ICS feed exceeds the {_MAX_ICS_BYTES}-byte limit"
                        )
                    chunks.append(chunk)
                etag_out = response.headers.get("ETag")
                return (
                    b"".join(chunks).decode("utf-8", "replace"),
                    etag_out,
                )
            finally:
                await response.aclose()
    raise ValueError(f"too many redirects (>{_MAX_REDIRECTS})")
