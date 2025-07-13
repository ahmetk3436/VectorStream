#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
live_event_sender.py – production-grade demo

• Python 3.10+
• PEP 8 / type hints
• Graceful shutdown + structured logging
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import FrameType

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from generate_ecommerce_data import ECommerceDataGenerator

# ────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────
def getenv(key: str, default: str | int) -> str:
    """Return env var or default; always str (lazy cast later)."""
    return os.getenv(key, str(default))


def _parse_acks(value: str) -> int | str:
    """Accept 'all', '-1', '1', etc.; return int or 'all'."""
    if value.lower() in {"all", "-1"}:
        return -1
    return int(value) if value.isdigit() else 1


# ────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────
logging.basicConfig(
    level=getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger("live_event_sender")

# ────────────────────────────────────────────
# Event generator (dummy)
# ────────────────────────────────────────────
class EventGeneratorProtocol:
    def generate_event(self) -> dict:  # noqa: D401
        raise NotImplementedError


class _DummyGenerator(EventGeneratorProtocol):
    def generate_event(self) -> dict:
        return {
            "id": datetime.now(tz=timezone.utc).isoformat(),
            "ts": time.time(),
            "note": "dummy-event",
        }


# ────────────────────────────────────────────
# Main sender
# ────────────────────────────────────────────
@dataclass
class LiveEventSender:
    generator: EventGeneratorProtocol = field(default_factory=ECommerceDataGenerator)
    kafka_servers: str = getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic: str = getenv("KAFKA_TOPIC", "ecommerce-events")

    health_endpoint: str = getenv("APP_HEALTH_URL", "http://localhost:8080/health")
    metrics_endpoint: str = getenv("APP_METRICS_URL", "http://localhost:8080/metrics")

    compression_type: str = getenv("KAFKA_COMPRESSION", "gzip")
    linger_ms: int = int(getenv("KAFKA_LINGER_MS", 1))
    batch_size: int = int(getenv("KAFKA_BATCH_SIZE", 32_768))
    buffer_memory: int = int(getenv("KAFKA_BUFFER_MEMORY", 64 * 1024 * 1024))

    _producer: KafkaProducer | None = field(init=False, default=None)
    _running: bool = field(init=False, default=False)
    acks: int | str = field(init=False)

    # ──────────────────────────────
    # Context manager
    # ──────────────────────────────
    def __enter__(self) -> "LiveEventSender":
        # acks önce hesaplanıyor —→ AttributeError yok
        self.acks = _parse_acks(getenv("KAFKA_ACKS", "1"))

        self._producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers.split(","),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            batch_size=self.batch_size,
            linger_ms=self.linger_ms,
            compression_type=self.compression_type,
            buffer_memory=self.buffer_memory,
            acks=self.acks,
            max_in_flight_requests_per_connection=5,
            retries=3,
        )
        logger.info("Kafka producer created (servers=%s, acks=%s)", self.kafka_servers, self.acks)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: D401
        if self._producer:
            try:
                self._producer.flush(timeout=10)
            finally:
                self._producer.close()
            logger.info("Kafka producer closed")

    # ──────────────────────────────
    # Public API
    # ──────────────────────────────
    def run(
        self,
        *,
        events_per_second: int,
        total_events: int,
        burst: bool = False,
        monitor_only: bool = False,
        stats_interval_sec: int = 5,
    ) -> None:
        self._running = True

        if monitor_only:
            self._monitor_loop(stats_interval_sec)
            return

        import threading

        monitor_thread = threading.Thread(
            target=self._monitor_loop,
            args=(stats_interval_sec,),
            daemon=True,
            name="metrics-monitor",
        )
        monitor_thread.start()

        try:
            if burst:
                logger.info("Running in BURST mode")
                self._send_burst(events_per_second, total_events)
            else:
                logger.info("Running in CONTINUOUS mode")
                self._send_continuous(events_per_second, total_events)
        finally:
            self._running = False
            monitor_thread.join()

    # ──────────────────────────────
    # Send loops
    # ──────────────────────────────
    def _send_continuous(self, eps: int, total: int) -> None:
        assert self._producer
        start, interval, next_ts, sent = time.time(), 1.0 / eps, time.time(), 0

        while sent < total and self._running:
            now = time.time()
            if now < next_ts:
                time.sleep(next_ts - now)

            self._handle_future(self._producer.send(self.topic, value=self.generator.generate_event()))
            sent += 1
            next_ts += interval

            if sent % 1_000 == 0:
                elapsed = time.time() - start
                logger.info("Progress: %d/%d events (%.1f ev/s)", sent, total, sent / elapsed)

    def _send_burst(self, eps: int, total: int) -> None:
        assert self._producer
        batch_size = max(1, min(100, eps // 10))
        start, sent = time.time(), 0
        events = [self.generator.generate_event() for _ in range(total)]

        for i in range(0, total, batch_size):
            if not self._running:
                break

            batch = events[i : i + batch_size]
            t0 = time.time()
            for evt in batch:
                self._handle_future(self._producer.send(self.topic, value=evt))
            sent += len(batch)

            # hız sınırlayıcı
            wait = (len(batch) / eps) - (time.time() - t0)
            if wait > 0:
                time.sleep(wait)

            if sent % 5_000 == 0:
                elapsed = time.time() - start
                logger.info("Progress: %d/%d events (%.1f ev/s)", sent, total, sent / elapsed)

    def _handle_future(self, future) -> None:
        future.add_errback(lambda exc: logger.error("Kafka send failed: %s", exc))

    # ──────────────────────────────
    # Monitoring
    # ──────────────────────────────
    def _monitor_loop(self, interval_sec: int) -> None:
        logger.info("Metrics monitor started (every %ss)", interval_sec)
        last_cons, last_proc = 0, 0
        while self._running:
            try:
                stats = self._fetch_metrics()
                d_cons = stats["consumed"] - last_cons
                d_proc = stats["processed"] - last_proc
                logger.info(
                    "Consumed=%d (+%d/s) | Processed=%d (+%d/s)",
                    stats["consumed"],
                    d_cons // interval_sec,
                    stats["processed"],
                    d_proc // interval_sec,
                )
                last_cons, last_proc = stats.values()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Metrics fetch failed: %s", exc, exc_info=False)
            time.sleep(interval_sec)

    def _fetch_metrics(self) -> dict[str, int]:
        resp = requests.get(self.metrics_endpoint, timeout=5)
        resp.raise_for_status()
        cons = proc = 0
        for line in resp.text.splitlines():
            if line.startswith("#"):
                continue
            if "newmind_ai_kafka_messages_consumed_total" in line:
                cons = int(float(line.split()[-1]))
            elif "newmind_ai_kafka_messages_processed_total" in line:
                proc = int(float(line.split()[-1]))
        return {"consumed": cons, "processed": proc}


# ────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────
def _build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Live Event Stream Demo")
    p.add_argument("-r", "--rate", type=int, default=100, help="events per second")
    p.add_argument("-c", "--count", type=int, default=5_000, help="total events")
    p.add_argument("--monitor-only", action="store_true", help="only monitor metrics")
    p.add_argument("--burst", action="store_true", help="enable burst mode")
    return p


def _main() -> None:
    args = _build_cli().parse_args()

    def _sigint(signum: int, frame: FrameType | None) -> None:  # noqa: D401
        logger.warning("Interrupt received → shutting down ...")
        sender._running = False  # noqa: SLF001

    signal.signal(signal.SIGINT, _sigint)

    with LiveEventSender() as sender:
        # Optional upfront health-check
        if not args.monitor_only:
            try:
                requests.get(sender.health_endpoint, timeout=3).raise_for_status()
            except requests.RequestException as exc:
                logger.error("Health check failed: %s", exc)
                sys.exit(1)

        sender.run(
            events_per_second=args.rate,
            total_events=args.count,
            burst=args.burst,
            monitor_only=args.monitor_only,
        )


if __name__ == "__main__":
    _main()
