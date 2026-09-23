from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class Settings:
    host: str = "0.0.0.0"
    port: int = 9999
    workers: int = 4
    queue_size: int = 10_000
    max_datagram_size: int = 1_400
    receive_buffer: int = 4 * 1024 * 1024
    rate_limit: int = 1_000
    database_url: str | None = None
    persist: bool = True

    @classmethod
    def from_args(cls) -> "Settings":
        p = argparse.ArgumentParser(description="Production asyncio UDP engine")
        p.add_argument("--host", default=os.getenv("UDP_HOST", cls.host))
        p.add_argument("--port", type=int, default=int(os.getenv("UDP_PORT", cls.port)))
        p.add_argument("--workers", type=int, default=cls.workers)
        p.add_argument("--queue-size", type=int, default=cls.queue_size)
        p.add_argument("--max-datagram-size", type=int, default=cls.max_datagram_size)
        p.add_argument("--receive-buffer", type=int, default=cls.receive_buffer)
        p.add_argument("--rate-limit", type=int, default=cls.rate_limit)
        p.add_argument("--db-url", default=os.getenv("DATABASE_URL"))
        p.add_argument("--no-persistence", action="store_true")
        a = p.parse_args()
        if not 1 <= a.port <= 65535 or min(a.workers, a.queue_size, a.max_datagram_size, a.rate_limit) < 1:
            p.error("invalid positive server settings")
        return cls(a.host, a.port, a.workers, a.queue_size, a.max_datagram_size, a.receive_buffer, a.rate_limit, a.db_url, not a.no_persistence)
