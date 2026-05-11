"""SSRF-safe ICS feed fetching.

Lifted out of the legacy ``app/sync/ics_parser.py`` so the API
layer and the new ledger webcal-ingest hook can both use it
without dragging in the rest of the legacy sync engine.
"""

from __future__ import annotations

import ipaddress
import logging
import socket
from typing import Optional
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

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
    try:
        addr = ipaddress.ip_address(ip_str)
    except ValueError:
        return True
    if (
        addr.is_private
        or addr.is_reserved
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_multicast
    ):
        return True
    for network in _BLOCKED_NETWORKS:
        if addr in network:
            return True
    return False


def validate_url_for_ssrf(url: str) -> None:
    """Raise ``ValueError`` if ``url`` targets a private/reserved network."""
    parsed = urlparse(url)
    hostname = parsed.hostname
    if not hostname:
        raise ValueError("URL has no hostname")
    try:
        if _is_ip_blocked(hostname):
            raise ValueError("URL points to a blocked address")
    except ValueError as e:
        # Re-raise our own "blocked address" string; suppress the
        # "not an IP" inner ValueError.
        if "blocked" in str(e):
            raise
    try:
        addrinfos = socket.getaddrinfo(
            hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM,
        )
    except socket.gaierror:
        raise ValueError(f"Cannot resolve hostname: {hostname}")
    if not addrinfos:
        raise ValueError(f"Cannot resolve hostname: {hostname}")
    for _family, _type, _proto, _canonname, sockaddr in addrinfos:
        ip = sockaddr[0]
        if _is_ip_blocked(ip):
            raise ValueError("URL resolves to a blocked address")


async def fetch_ics_feed(
    url: str,
    etag: Optional[str] = None,
    timeout: float = 30.0,
) -> tuple[Optional[str], Optional[str]]:
    """SSRF-safe ICS fetch returning ``(content, new_etag)`` or
    ``(None, None)`` on 304 Not Modified."""
    if url.startswith("webcal://"):
        url = "https://" + url[len("webcal://"):]
    validate_url_for_ssrf(url)
    headers = {}
    if etag:
        headers["If-None-Match"] = etag
    async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
        response = await client.get(url, headers=headers)
    final_url = str(response.url)
    if final_url != url:
        validate_url_for_ssrf(final_url)
    if response.status_code == 304:
        return None, None
    response.raise_for_status()
    return response.text, response.headers.get("ETag")
