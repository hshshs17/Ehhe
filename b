#!/usr/bin/env python3
"""
High-performance asyncio UDP server with PostgreSQL persistence.
"""
import argparse
import asyncio
import logging
import socket
import signal
import time
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor

from database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("udp-server")


class UDPServerProtocol(asyncio.DatagramProtocol):
    __slots__ = ("queue", "stats", "logs_drops", "transport")

    def __init__(self, queue: asyncio.Queue, stats: Dict[str, Any]):
        self.queue = queue
        self.stats = stats
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        logger.info("UDP transport ready")

    def datagram_received(self, data: bytes, addr):
        self.stats["recv"] += 1
        self.stats["bytes_recv"] += len(data)
        try:
            self.queue.put_nowait((memoryview(data), addr, time.time()))
        except asyncio.QueueFull:
            self.stats["dropped"] += 1
            if self.stats.get("logs_drops"):
                logger.warning("Queue full, dropping packet from %s", addr)

    def error_received(self, exc):
        logger.error("Socket error: %s", exc)
        self.stats["errors"] += 1

    def connection_lost(self, exc):
        logger.info("Connection lost: %s", exc)


def cpu_task(data: bytes) -> bytes:
    return data[::-1]


async def worker_main(worker_id, queue, protocol, stats, stop_event, executor, sem, database):
    loop = asyncio.get_running_loop()
    logger.info("Worker %d started", worker_id)
    while not stop_event.is_set():
        try:
            item = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        data, addr, ts = item
        try:
            async with sem:
                processed_data = await loop.run_in_executor(executor, cpu_task, bytes(data))

            process_ts = time.time()
            latency_ms = int((process_ts - ts) * 1000)
            await database.save_packet(bytes(data), processed_data, addr, ts, latency_ms)

            resp = b"OK %dms %b" % (latency_ms, bytes(data[:200]))
            if protocol.transport is not None:
                protocol.transport.sendto(resp, addr)
            stats["processed"] += 1
            stats["bytes_sent"] += len(resp)
        except Exception as exc:
            stats["errors"] += 1
            logger.exception("Worker %d processing/database error: %s", worker_id, exc)
        finally:
            queue.task_done()
    logger.info("Worker %d stopping", worker_id)


async def stats_reporter(queue, stats, interval, stop_event):
    last = time.time()
    prev_recv = prev_proc = prev_dropped = prev_bytes = prev_sent = 0
    while not stop_event.is_set():
        await asyncio.sleep(interval)
        now = time.time()
        elapsed = max(now - last, 0.001)
        last = now
        recv, proc = stats["recv"], stats["processed"]
        dropped, errors = stats["dropped"], stats["errors"]
        bytes_recv, bytes_sent = stats["bytes_recv"], stats["bytes_sent"]
        logger.info(
            "stats: recv/s=%.1f proc/s=%.1f dropped/s=%.1f errors=%d in/s=%.1fKB/s out/s=%.1fKB/s queue_current=%d",
            (recv - prev_recv) / elapsed, (proc - prev_proc) / elapsed,
            (dropped - prev_dropped) / elapsed, errors,
            (bytes_recv - prev_bytes) / elapsed / 1024,
            (bytes_sent - prev_sent) / elapsed / 1024, queue.qsize(),
        )
        prev_recv, prev_proc, prev_dropped = recv, proc, dropped
        prev_bytes, prev_sent = bytes_recv, bytes_sent


def build_socket(host: str, port: int, recv_buf: int, reuse_port: bool):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if reuse_port and hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, recv_buf)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, recv_buf)
    except Exception:
        logger.warning("Could not set socket buffer sizes")
    sock.bind((host, port))
    return sock


async def main():
    parser = argparse.ArgumentParser(description="High-performance asyncio UDP server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--queue", type=int, default=10000)
    parser.add_argument("--recv-buf", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--stats-interval", type=float, default=5.0)
    parser.add_argument("--reuse-port", action="store_true")
    parser.add_argument("--uvloop", action="store_true")
    parser.add_argument("--concurrency", type=int, default=7)
    parser.add_argument("--db-url", default=None, help="PostgreSQL DSN; defaults to DATABASE_URL")
    args = parser.parse_args()

    if args.uvloop:
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            logger.info("uvloop enabled")
        except Exception:
            logger.warning("uvloop not available, continuing with default loop")

    loop = asyncio.get_running_loop()
    stats = {"recv": 0, "processed": 0, "dropped": 0, "errors": 0,
             "bytes_recv": 0, "bytes_sent": 0, "logs_drops": False}
    queue = asyncio.Queue(maxsize=args.queue)
    stop_event = asyncio.Event()
    executor = ThreadPoolExecutor(max_workers=args.workers)
    sem = asyncio.Semaphore(args.concurrency)
    database = Database(args.db_url, max_size=max(args.workers, args.concurrency))
    await database.connect()
    logger.info("PostgreSQL connected")

    sock = build_socket(args.host, args.port, args.recv_buf, args.reuse_port)
    protocol = UDPServerProtocol(queue, stats)
    transport, _ = await loop.create_datagram_endpoint(lambda: protocol, sock=sock)
    workers = [asyncio.create_task(worker_main(i, queue, protocol, stats, stop_event, executor, sem, database)) for i in range(args.workers)]
    stats_task = asyncio.create_task(stats_reporter(queue, stats, args.stats_interval, stop_event))

    def _on_signal():
        logger.info("Shutdown signal received")
        stop_event.set()

    try:
        loop.add_signal_handler(signal.SIGINT, _on_signal)
        loop.add_signal_handler(signal.SIGTERM, _on_signal)
    except NotImplementedError:
        logger.warning("Signal handlers not fully supported on this platform")

    logger.info("UDP server listening on %s:%d", args.host, args.port)
    await stop_event.wait()
    try:
        await asyncio.wait_for(queue.join(), timeout=5.0)
    except asyncio.TimeoutError:
        logger.warning("Timeout waiting for queue to drain")
    for worker in workers:
        worker.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
    stats_task.cancel()
    await asyncio.gather(stats_task, return_exceptions=True)
    transport.close()
    executor.shutdown(wait=True)
    await database.close()
    logger.info("Server shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted, exiting")
