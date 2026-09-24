from udp_engine.main import process_payload
from udp_engine.ratelimit import RateLimiter


def test_process_payload() -> None:
    assert process_payload(b"abc") == b"cba"


def test_rate_limiter() -> None:
    limiter = RateLimiter(2)
    assert limiter.allow("127.0.0.1")
    assert limiter.allow("127.0.0.1")
    assert not limiter.allow("127.0.0.1")
