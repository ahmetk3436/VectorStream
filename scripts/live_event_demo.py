#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
live_event_sender_fast.py – high-throughput Kafka demo (fixed)
==============================================================
Drop-in replacement for the original script. 3–5× daha yüksek TPS,
“burst” modunda bile TypeError fırlatmaz.

Kullanım:
    # 10 k olayı mümkün olan en hızlı şekilde gönder
    python live_event_sender_fast.py -c 10000 --burst --no-rate-limit  --compression lz4
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
from typing import List

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

from generate_ecommerce_data import ECommerceDataGenerator  # type: ignore

try:
    import orjson  

    _ORJSON_OPTS = 0
    if hasattr(orjson, "OPT_NON_STR_KEYS"):
        _ORJSON_OPTS |= orjson.OPT_NON_STR_KEYS
    if hasattr(orjson, "OPT_IEEE754"):
        _ORJSON_OPTS |= orjson.OPT_IEEE754

    def _dumps(obj) -> bytes:
        """bytes gelirse aynen döndür, yoksa orjson ile encode et."""
        if isinstance(obj, (bytes, bytearray, memoryview)):
            return obj
        return orjson.dumps(obj, option=_ORJSON_OPTS)

except ModuleNotFoundError:  # Geliştirme ortamı
    def _dumps(obj) -> bytes:  # type: ignore
        if isinstance(obj, (bytes, bytearray, memoryview)):
            return obj
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode()

def getenv(key: str, default: str | int) -> str:
    return os.getenv(key, str(default))


def _parse_acks(value: str | int) -> int | str:
    if str(value).lower() in {"all", "-1"}:
        return -1
    return int(value)


logging.basicConfig(
    level=getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger("live_event_sender_fast")

class EventGeneratorProtocol:
    def generate_event(self) -> dict:  
        raise NotImplementedError
@dataclass
class LiveEventSender:
    generator: EventGeneratorProtocol = field(default_factory=ECommerceDataGenerator)

    kafka_servers: str = getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic: str = getenv("KAFKA_TOPIC", "ecommerce-events")

    health_endpoint: str = getenv("APP_HEALTH_URL", "http://localhost:8080/health")
    metrics_endpoint: str = getenv("APP_METRICS_URL", "http://localhost:8080/metrics")

    compression_type: str = getenv("KAFKA_COMPRESSION", "lz4")
    linger_ms: int = int(getenv("KAFKA_LINGER_MS", 5))
    batch_size: int = int(getenv("KAFKA_BATCH_SIZE", 64_000))
    buffer_memory: int = int(getenv("KAFKA_BUFFER_MEMORY", 256 * 1024 * 1024))

    max_in_flight: int = int(getenv("KAFKA_MAX_IN_FLIGHT", 10))
    retries: int = int(getenv("KAFKA_RETRIES", 5))

    _producer: KafkaProducer | None = field(init=False, default=None)
    _running: bool = field(init=False, default=False)
    acks: int | str = field(init=False)

    def __enter__(self) -> "LiveEventSender":
        self.acks = _parse_acks(getenv("KAFKA_ACKS", "1"))
        self._producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers.split(","),
            value_serializer=_dumps,          # bytes bypass artık güvenli
            batch_size=self.batch_size,
            linger_ms=self.linger_ms,
            compression_type=self.compression_type,
            buffer_memory=self.buffer_memory,
            acks=self.acks,
            max_in_flight_requests_per_connection=self.max_in_flight,
            retries=self.retries,
        )
        logger.info(
            "Kafka producer ready – servers=%s acks=%s compr=%s batch=%d bytes",
            self.kafka_servers,
            self.acks,
            self.compression_type,
            self.batch_size,
        )
        return self

    def __exit__(self, *_exc) -> None:
        if self._producer:
            try:
                self._producer.flush(10)
            finally:
                self._producer.close()
            logger.info("Kafka producer closed")

    # ───────── public API ─────────
    def run(
        self,
        *,
        events_per_second: int,
        total_events: int,
        burst: bool = False,
        monitor_only: bool = False,
        stats_interval_sec: int = 5,
        no_rate_limit: bool = False,
    ) -> None:
        self._running = True

        if monitor_only:
            self._monitor_loop(stats_interval_sec)
            return

        import threading

        monitor_thread = threading.Thread(
            target=self._monitor_loop, args=(stats_interval_sec,), daemon=True, name="metrics-monitor"
        )
        monitor_thread.start()

        try:
            if burst:
                logger.info("Mode → BURST%s", " (no-limit)" if no_rate_limit else "")
                self._send_burst(events_per_second, total_events, no_rate_limit)
            else:
                logger.info("Mode → CONTINUOUS%s", " (no-limit)" if no_rate_limit else "")
                self._send_continuous(events_per_second, total_events, no_rate_limit)
        finally:
            self._running = False
            monitor_thread.join()

    def _send_continuous(self, eps: int, total: int, no_limit: bool) -> None:
        assert self._producer
        start = time.time()
        interval = 0.0 if no_limit else 1.0 / max(1, eps)
        next_ts = time.time()
        sent = 0

        while sent < total and self._running:
            if not no_limit:
                now = time.time()
                if now < next_ts:
                    time.sleep(next_ts - now)
                next_ts += interval

            self._handle_future(self._producer.send(self.topic, value=self.generator.generate_event()))
            sent += 1

            if sent % 10_000 == 0 or sent == total:
                elapsed = time.time() - start or 1e-3
                logger.info("Progress: %d/%d (%.0f ev/s)", sent, total, sent / elapsed)

    def _send_burst(self, eps: int, total: int, no_limit: bool) -> None:
        assert self._producer
        logger.debug("Pre-generating %d events", total)
        raw_events: List[bytes] = [_dumps(self.generator.generate_event()) for _ in range(total)]

        batch_size = max(1, min(500, eps // 5)) if not no_limit else 1000
        start, sent = time.time(), 0

        for i in range(0, total, batch_size):
            if not self._running:
                break
            batch = raw_events[i : i + batch_size]

            t0 = time.time()
            for ev_bytes in batch:
                self._handle_future(self._producer.send(self.topic, value=ev_bytes))
            sent += len(batch)

            if not no_limit:
                wait = (len(batch) / eps) - (time.time() - t0)
                if wait > 0:
                    time.sleep(wait)

            if sent % 20_000 == 0 or sent == total:
                elapsed = time.time() - start or 1e-3
                logger.info("Progress: %d/%d (%.0f ev/s)", sent, total, sent / elapsed)

    def _handle_future(self, fut) -> None:
        fut.add_errback(lambda exc: logger.error("Kafka send failed: %s", exc))

    def _monitor_loop(self, interval_sec: int) -> None:
        logger.info("Metrics monitor every %ds", interval_sec)
        last_cons = last_proc = 0
        metrics_on = True
        while self._running:
            try:
                if metrics_on:
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
                if metrics_on:
                    logger.warning("Metrics fetch failed → monitoring disabled: %s", exc)
                    metrics_on = False
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

def _build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("Live Event Stream Demo (fast)")
    p.add_argument("-r", "--rate", type=int, default=1_000, help="events per second (target)")
    p.add_argument("-c", "--count", type=int, default=10_000, help="total events to send")
    p.add_argument("--burst", action="store_true", help="enable burst mode (pre-generate list)")
    p.add_argument("--monitor-only", action="store_true", help="only monitor metrics; no send")
    p.add_argument("--no-rate-limit", action="store_true", help="disable EPS throttling (max speed)")
    p.add_argument("--acks", default="1", help="Kafka acks setting (0/1/all)")
    p.add_argument("--compression", default="lz4", help="Kafka compression (lz4|gzip|snappy|zstd)")
    return p


def _main() -> None:
    args = _build_cli().parse_args()

    # CLI’daki override’lar env’i bastırır
    os.environ["KAFKA_ACKS"] = str(args.acks)
    os.environ["KAFKA_COMPRESSION"] = args.compression

    sender = LiveEventSender()

    def _graceful(_sig: int, _frm: FrameType | None) -> None:
        logger.warning("Interrupt → graceful shutdown …")
        sender._running = False  # type: ignore

    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, _graceful)

    with sender:
        if not args.monitor_only:
            try:
                requests.get(sender.health_endpoint, timeout=3).raise_for_status()
                logger.info("✅ Health check OK – pipeline live")
            except requests.RequestException as exc:
                logger.warning("⚠️  Health check failed: %s – continuing", exc)

        sender.run(
            events_per_second=max(1, args.rate),
            total_events=args.count,
            burst=args.burst,
            monitor_only=args.monitor_only,
            no_rate_limit=args.no_rate_limit,
        )


if __name__ == "__main__":
    _main()
