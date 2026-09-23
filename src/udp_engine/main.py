from __future__ import annotations
import asyncio, logging, signal, socket, time
from .config import Settings
from .protocol import Packet, UDPProtocol
from .ratelimit import RateLimiter

logger = logging.getLogger("udp-engine")
def process_payload(data: bytes) -> bytes: return data[::-1]

def build_socket(s: Settings) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, s.receive_buffer)
    sock.bind((s.host, s.port)); return sock

async def worker(queue, protocol, stats, stop):
    while True:
        packet: Packet = await queue.get()
        try:
            processed = process_payload(packet.data)
            latency = max(0, int((time.time() - packet.received_at) * 1000))
            response = b"OK %dms %b" % (latency, processed[:200])
            if protocol.transport: protocol.transport.sendto(response, packet.address)
            stats["processed"] += 1; stats["bytes_sent"] += len(response)
        except asyncio.CancelledError: raise
        except Exception: stats["errors"] += 1; logger.exception("packet processing failed")
        finally: queue.task_done()

async def run(settings: Settings) -> None:
    stats = {k: 0 for k in ("received", "processed", "dropped", "invalid", "rate_limited", "errors", "bytes_received", "bytes_sent")}
    queue = asyncio.Queue(settings.queue_size); stop = asyncio.Event()
    loop = asyncio.get_running_loop(); protocol = UDPProtocol(queue, stats, settings.max_datagram_size, RateLimiter(settings.rate_limit))
    transport, _ = await loop.create_datagram_endpoint(lambda: protocol, sock=build_socket(settings))
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: loop.add_signal_handler(sig, stop.set)
        except NotImplementedError: pass
    tasks = [asyncio.create_task(worker(queue, protocol, stats, stop)) for _ in range(settings.workers)]
    logger.info("listening on %s:%d workers=%d", settings.host, settings.port, settings.workers)
    try: await stop.wait()
    finally:
        transport.close()
        try: await asyncio.wait_for(queue.join(), 5)
        except asyncio.TimeoutError: logger.warning("queue did not drain")
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("shutdown complete stats=%s", stats)

def main() -> None:
    settings = Settings.from_args()
    try:
        import uvloop; uvloop.install()
    except ImportError: pass
    asyncio.run(run(settings))

if __name__ == "__main__": main()
