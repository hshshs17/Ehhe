from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from .ratelimit import RateLimiter


@dataclass(frozen=True, slots=True)
class Packet:
    data: bytes
    address: tuple[str, int]
    received_at: float


class UDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, queue: asyncio.Queue[Packet], stats: dict[str, int], max_size: int, limiter: RateLimiter):
        self.queue = queue
        self.stats = stats
        self.max_size = max_size
        self.limiter = limiter
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: Any) -> None:
        self.stats["received"] += 1
        self.stats["bytes_received"] += len(data)
        if len(data) > self.max_size:
            self.stats["invalid"] += 1
            return
        if not self.limiter.allow(str(addr[0])):
            self.stats["rate_limited"] += 1
            return
        try:
            self.queue.put_nowait(Packet(bytes(data), (str(addr[0]), int(addr[1])), time.time()))
        except asyncio.QueueFull:
            self.stats["dropped"] += 1

    def error_received(self, exc: Exception) -> None:
        self.stats["errors"] += 1
