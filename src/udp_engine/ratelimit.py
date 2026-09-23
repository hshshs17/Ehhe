from __future__ import annotations
import time
from collections import defaultdict, deque

class RateLimiter:
    def __init__(self, limit: int, window: float = 1.0) -> None:
        self.limit, self.window = limit, window
        self.events: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, source: str) -> bool:
        now = time.monotonic(); events = self.events[source]
        while events and events[0] <= now - self.window: events.popleft()
        if len(events) >= self.limit: return False
        events.append(now); return True
