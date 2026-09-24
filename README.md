# Asyncio UDP Engine

A production-oriented asyncio UDP service with bounded backpressure, packet validation, per-source rate limiting, graceful shutdown, runtime statistics, Docker deployment, and asynchronous PostgreSQL batch persistence.

## Run without PostgreSQL

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
PYTHONPATH=src python -m udp_engine.main --no-persistence --port 9999
```

## Run with PostgreSQL

```bash
docker compose up --build
```

The receive callback performs only validation, rate limiting, and a non-blocking queue operation. Workers process packets and enqueue database rows separately; a batch persister writes rows without blocking UDP reception. Queue limits intentionally drop excess datagrams instead of exhausting memory.

## Production checklist

Before exposing this service to an untrusted network, add an authenticated application protocol (for example HMAC with replay protection), Prometheus/OpenTelemetry metrics, firewall rules, load tests, and secret management. UDP is connectionless and does not provide delivery guarantees.
