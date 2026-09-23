from udp_engine.ratelimit import RateLimiter
from udp_engine.main import process_payload

def test_payload():
    assert process_payload(b"abc") == b"cba"

def test_rate_limit():
    limiter = RateLimiter(2)
    assert limiter.allow("127.0.0.1")
    assert limiter.allow("127.0.0.1")
    assert not limiter.allow("127.0.0.1")
