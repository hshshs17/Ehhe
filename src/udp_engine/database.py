from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Iterable

from psycopg_pool import AsyncConnectionPool

from .protocol import Packet

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS udp_packets (
    id BIGSERIAL PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL,
    sender_ip INET NOT NULL,
    sender_port INTEGER NOT NULL,
    raw_data BYTEA NOT NULL,
    processed_data BYTEA NOT NULL,
    latency_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS udp_packets_received_at_idx
ON udp_packets (received_at DESC);
"""

INSERT_PACKET = """
INSERT INTO udp_packets
(received_at, sender_ip, sender_port, raw_data, processed_data, latency_ms)
VALUES (%s, %s, %s, %s, %s, %s)
"""


class Database:
    def __init__(self, dsn: str | None, max_size: int = 10) -> None:
        self.dsn = dsn or os.getenv("DATABASE_URL")
        if not self.dsn:
            raise ValueError("DATABASE_URL is required when persistence is enabled")
        self.pool = AsyncConnectionPool(
            conninfo=self.dsn, min_size=1, max_size=max_size, open=False
        )

    async def connect(self) -> None:
        await self.pool.open()
        await self.pool.wait()
        async with self.pool.connection() as connection:
            await connection.execute(CREATE_TABLE)

    async def save_batch(self, rows: Iterable[tuple]) -> None:
        async with self.pool.connection() as connection:
            await connection.executemany(INSERT_PACKET, list(rows))

    async def close(self) -> None:
        await self.pool.close()


def make_row(packet: Packet, processed: bytes, latency_ms: int) -> tuple:
    received_at = datetime.fromtimestamp(packet.received_at, tz=timezone.utc)
    return (
        received_at,
        packet.address[0],
        packet.address[1],
        packet.data,
        processed,
        latency_ms,
    )
