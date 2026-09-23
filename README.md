# Asyncio UDP Engine

Production-oriented UDP server with bounded backpressure, packet-size validation, per-source rate limiting, graceful shutdown, structured package layout, and Docker support.

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
PYTHONPATH=src python -m udp_engine.main --port 9999
```

The receiver performs only fast validation and a non-blocking queue operation. Heavy persistence should be added as a separate batch pipeline so database latency never blocks UDP reception. Add authenticated protocol messages, HMAC, metrics, load tests, and PostgreSQL batching before exposing it to an untrusted network.
