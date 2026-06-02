"""The Google webhook endpoint must rate-limit per push channel, not per
IP. All of Google's notifications arrive from a small IP pool (a single
IP behind a proxy), so an IP key makes every channel share one bucket and
a burst 429s real notifications. Keying on the channel id gives each
channel its own bucket.
"""

from app.rate_limit import webhook_rate_key


class _Req:
    def __init__(self, headers=None, host="9.9.9.9"):
        self.headers = headers or {}
        self.client = type("C", (), {"host": host})()


def test_key_uses_channel_id_when_present():
    assert webhook_rate_key(_Req({"X-Goog-Channel-ID": "chanA"})) == "wh-chan:chanA"


def test_distinct_channels_get_distinct_keys():
    a = webhook_rate_key(_Req({"X-Goog-Channel-ID": "chanA"}))
    b = webhook_rate_key(_Req({"X-Goog-Channel-ID": "chanB"}))
    assert a != b, "each channel must get its own rate-limit bucket"


def test_falls_back_to_ip_without_channel_id():
    # No channel header (malformed/non-Google request) → key on the IP,
    # which the handler then rejects with 400.
    assert webhook_rate_key(_Req(host="9.9.9.9")) == "9.9.9.9"
