from __future__ import annotations

import asyncio
import logging
import signal
import socket
import time
from collections.abc import Awaitable, Callable

from .config import Settings
from .database import Database, make_row
from .protocol import Packet, UDPProtocol
from .ratelimit import RateLimiter

logger = logging.getLogger("udp-engine")
Processor = Callable[[bytes], bytes]


def process_payload(data: bytes) -> bytes:
    """Reference processor; replace with application-specific logic."""
    return data[::-1]


def build_socket(settings: Settings) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, settings.receive_buffer)
    sock.bind((settings.host, settings.port))
    return sock


async def batch_persister(
    database: Database,
    queue: asyncio.Queue[tuple],
    batch_size: int,
    flush_interval: float,
    stats: dict[str, int],
) -> None:
    """Persist rows in batches without blocking the UDP receive callback."""
    while True:
        first = await queue.get()
        batch = [first]
        deadline = time.monotonic() + flush_interval
        try:
            while len(batch) < batch_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    batch.append(await asyncio.wait_for(queue.get(), remaining))
                except asyncio.TimeoutError:
                    break
            await database.save_batch(batch)
            stats["persisted"] += len(batch)
        except asyncio.CancelledError:
            raise
        except Exception:
            stats["persistence_errors"] += 1
            logger.exception("database batch write failed; batch_size=%d", len(batch))
        finally:
            for _ in batch:
                queue.task_done()


async def worker(
    queue: asyncio.Queue[Packet],
    persist_queue: asyncio.Queue[tuple],
    protocol: UDPProtocol,
    stats: dict[str, int],
    persist: bool,
    processor: Processor = process_payload,
) -> None:
    while True:
        packet = await queue.get()
        try:
            processed = processor(packet.data)
            latency_ms = max(0, int((time.time() - packet.received_at) * 1000))
            if persist:
                try:
                    persist_queue.put_nowait(make_row(packet, processed, latency_ms))
                except asyncio.QueueFull:
                    stats["persistence_dropped"] += 1
            response = b"OK %dms %b" % (latency_ms, processed[:200])
            if protocol.transport is not None:
                protocol.transport.sendto(response, packet.address)
                stats["bytes_sent"] += len(response)
            stats["processed"] += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            stats["errors"] += 1
            logger.exception("packet processing failed")
        finally:
            queue.task_done()


async def stats_reporter(
    queue: asyncio.Queue[Packet],
    stats: dict[str, int],
    interval: float,
    stop: asyncio.Event,
) -> None:
    previous = dict(stats)
    previous_at = time.monotonic()
    while not stop.is_set():
        await asyncio.sleep(interval)
        now = time.monotonic()
        elapsed = max(now - previous_at, 0.001)
        logger.info(
            "stats recv/s=%.1f processed/s=%.1f dropped=%d invalid=%d "
            "rate_limited=%d errors=%d queue=%d persisted=%d",
            (stats["received"] - previous["received"]) / elapsed,
            (stats["processed"] - previous["processed"]) / elapsed,
            stats["dropped"], stats["invalid"], stats["rate_limited"],
            stats["errors"], queue.qsize(), stats["persisted"],
        )
        previous, previous_at = dict(stats), now


async def run(settings: Settings) -> None:
    keys = (
        "received", "processed", "dropped", "invalid", "rate_limited", "errors",
        "bytes_received", "bytes_sent", "persisted", "persistence_errors",
        "persistence_dropped",
    )
    stats = {key: 0 for key in keys}
    packet_queue: asyncio.Queue[Packet] = asyncio.Queue(settings.queue_size)
    persist_queue: asyncio.Queue[tuple] = asyncio.Queue(settings.queue_size)
    stop = asyncio.Event()
    database = Database(settings.database_url, max_size=settings.workers) if settings.persist else None
    if database:
        await database.connect()

    loop = asyncio.get_running_loop()
    protocol = UDPProtocol(packet_queue, stats, settings.max_datagram_size, RateLimiter(settings.rate_limit))
    transport, _ = await loop.create_datagram_endpoint(lambda: protocol, sock=build_socket(settings))
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass

    workers = [asyncio.create_task(worker(packet_queue, persist_queue, protocol, stats, database is not None)) for _ in range(settings.workers)]
    persister = asyncio.create_task(batch_persister(database, persist_queue, settings.db_batch_size, settings.db_flush_interval, stats)) if database else None
    reporter = asyncio.create_task(stats_reporter(packet_queue, stats, settings.stats_interval, stop))
    logger.info("listening host=%s port=%d workers=%d queue=%d", settings.host, settings.port, settings.workers, settings.queue_size)
    try:
        await stop.wait()
    finally:
        transport.close()
        try:
            await asyncio.wait_for(packet_queue.join(), timeout=5)
        except asyncio.TimeoutError:
            logger.warning("packet queue did not drain before shutdown")
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        if persister:
            try:
                await asyncio.wait_for(persist_queue.join(), timeout=5)
            except asyncio.TimeoutError:
                logger.warning("persistence queue did not drain before shutdown")
            persister.cancel()
            await asyncio.gather(persister, return_exceptions=True)
        reporter.cancel()
        await asyncio.gather(reporter, return_exceptions=True)
        if database:
            await database.close()
        logger.info("shutdown complete stats=%s", stats)


def main() -> None:
    settings = Settings.from_args()
    if settings.use_uvloop:
        try:
            import uvloop
            uvloop.install()
        except ImportError:
            logger.warning("uvloop requested but unavailable")
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
