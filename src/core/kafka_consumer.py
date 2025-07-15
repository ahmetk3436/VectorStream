#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka Consumer (high-performance)
---------------------------------
* confluent-kafka C client  ➜  50k-60k msg/s
* orjson                    ➜  ~3× daha hızlı JSON ayrıştırma
* Circuit-breaker + DLQ     ➜  üretim hatası toleransı
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import orjson
from confluent_kafka import Consumer as CfConsumer
from confluent_kafka import KafkaError, KafkaException
from loguru import logger

# — Proje içi yollar --------------------------------------------------------
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from src.config.kafka_config import KafkaConfig
from src.utils.circuit_breaker import (
    CircuitBreakerConfig,
    CircuitBreakerError,
    circuit_breaker_manager,
)
from src.utils.dead_letter_queue import FailureReason, get_dlq

# ---------------------------------------------------------------------------


def _safe_decode(value: bytes | str | None) -> Optional[str]:
    """Kafka key/value → str (UTF-8 decode hatasına dayanıklı)."""
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return str(value)
    return str(value)


class KafkaConsumer:
    """Ultra hızlı Kafka tüketicisi (confluent-kafka)."""

    def __init__(self, config: KafkaConfig) -> None:
        self.cfg = config
        self.consumer: Optional[CfConsumer] = None
        self.handler: Optional[Callable[[Dict[str, Any]], Any]] = None
        self.running = False

        # metrikler
        self.start_time: float | None = None
        self.msg_total = 0

        # circuit breaker
        self.cb = circuit_breaker_manager.create_circuit_breaker(
            "kafka_consumer_cf",
            CircuitBreakerConfig(
                failure_threshold=5,
                recovery_timeout=10.0,
                timeout=15.0,
                success_threshold=2,
            ),
        )

        self.dlq = get_dlq()

    # ------------------------------------------------------------------ API

    def set_message_handler(self, fn: Callable[[Dict[str, Any]], Any]) -> None:
        """Her mesaj için çağrılacak kullanıcı işlevini tanımlar."""
        self.handler = fn
    
    def set_bulk_message_handler(self, fn: Callable[[list], Any]) -> None:
        """Bulk event işleme için çağrılacak kullanıcı işlevini tanımlar."""
        self.handler = fn

    async def start_consuming(self) -> None:
        """Ana tüketim döngüsünü başlatır (sonsuz) - bulk message ingestion."""
        await self._init_consumer()
        self.running, self.start_time = True, time.time()
        logger.info("🚀 Kafka consumer (confluent-kafka) başladı - bulk mode.")

        while self.running:
            try:
                # Bulk message polling - ingest messages in bulk
                msgs = self.consumer.consume(num_messages=5000, timeout=0.1)
                if not msgs:
                    await asyncio.sleep(0.01)
                    continue

                # Filter out error messages and collect valid messages
                valid_msgs = []
                for msg in msgs:
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        logger.error(f"Kafka message error: {msg.error()}")
                        continue
                    valid_msgs.append(msg)

                if valid_msgs:
                    # Submit the entire list to the pipeline *once*
                    await self.cb.call(self._process_messages, valid_msgs)

            except CircuitBreakerError as e:
                logger.error(f"Circuit-breaker açık: {e}")
                # Send all messages in current batch to DLQ
                if 'msgs' in locals() and msgs:
                    for msg in msgs:
                        if not msg.error():
                            await self._to_dlq(msg, FailureReason.CIRCUIT_BREAKER_OPEN, str(e))
                await asyncio.sleep(1)

            except Exception as e:
                logger.exception(f"Tüketici döngü hatası: {e}")
                await asyncio.sleep(1)

        self._close_consumer()

    async def close(self) -> None:
        """Haricî olarak tüketiciyi durdurur."""
        self.running = False

    # ----------------------------------------------------------- İç yardımcı

    async def _init_consumer(self) -> None:
        cfg = {
            "bootstrap.servers": self.cfg.bootstrap_servers,
            "group.id": self.cfg.group_id,
            "auto.offset.reset": self.cfg.auto_offset_reset,
            "enable.auto.commit": self.cfg.enable_auto_commit,
            # IPv4 zorlaması
            "broker.address.family": "v4",
            # yüksek performans ayarları
            "fetch.max.bytes": 32 * 1024 * 1024,  # 32 MB
            "fetch.wait.max.ms": 100,
            "session.timeout.ms": 30_000,
            "max.poll.interval.ms": 300_000,
        }
        self.consumer = CfConsumer(cfg)
        self.consumer.subscribe([self.cfg.topic])

    async def _process_messages(self, msgs) -> None:  # noqa: ANN001
        """Bulk message processing - processes multiple Kafka messages at once."""
        if not msgs:
            return
            
        # Parse all messages as events in bulk using vectorized orjson operations
        events = []
        failed_msgs = []
        
        for msg in msgs:
            try:
                # Use orjson for fast JSON parsing - convert to events
                # msg.value can be either bytes (in tests) or callable (in real Kafka)
                value = msg.value() if callable(msg.value) else msg.value
                event = orjson.loads(value)
                events.append(event)
            except orjson.JSONDecodeError as e:
                logger.error(f"JSON parse hatası: {e}")
                failed_msgs.append((msg, e))
                continue
        
        # Send failed messages to DLQ
        for msg, error in failed_msgs:
            await self._to_dlq(msg, FailureReason.VALIDATION_ERROR, str(error))
        
        # Process all events in bulk if handler exists
        if self.handler and events:
            if asyncio.iscoroutinefunction(self.handler):
                # Call handler with event batch - this should be _process_event_batch
                await self.handler(events)
            else:
                # Synchronous handler - use thread pool for bulk processing
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.handler, events)
        
        # Update metrics for bulk processing
        self.msg_total += len(events)
        if self.msg_total % 10_000 == 0:
            elapsed = time.time() - self.start_time  # type: ignore[arg-type]
            rate = self.msg_total / elapsed if elapsed else 0
            logger.info(f"📈 Ortalama hız: {rate:,.0f} msg/s (bulk: {len(events)} events)")
    
    async def _process_message(self, msg) -> None:  # noqa: ANN001
        """Tek bir Kafka mesajını işler - legacy single message processing."""
        # JSON parse
        try:
            # msg.value can be either bytes (in tests) or callable (in real Kafka)
            value = msg.value() if callable(msg.value) else msg.value
            data = orjson.loads(value)
        except orjson.JSONDecodeError as e:
            logger.error(f"JSON parse hatası: {e}")
            await self._to_dlq(msg, FailureReason.VALIDATION_ERROR, str(e))
            return

        # kullanıcı handler çağrısı
        if self.handler:
            if asyncio.iscoroutinefunction(self.handler):
                await self.handler(data)
            else:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.handler, data)

        # metrik
        self.msg_total += 1
        if self.msg_total % 10_000 == 0:
            elapsed = time.time() - self.start_time  # type: ignore[arg-type]
            rate = self.msg_total / elapsed if elapsed else 0
            logger.info(f"📈 Ortalama hız: {rate:,.0f} msg/s")

    async def _to_dlq(
        self, msg, reason: FailureReason, err: str  # noqa: ANN001
    ) -> None:
        """Mesajı DLQ kuyruğuna yollar."""
        # Handle both callable and non-callable msg attributes for test compatibility
        topic = msg.topic() if callable(msg.topic) else msg.topic
        partition = msg.partition() if callable(msg.partition) else msg.partition
        offset = msg.offset() if callable(msg.offset) else msg.offset
        key = msg.key() if callable(msg.key) else msg.key
        value = msg.value() if callable(msg.value) else msg.value
        
        await self.dlq.send_to_dlq(
            original_topic=topic,
            original_partition=partition,
            original_offset=offset,
            original_key=_safe_decode(key),
            original_value=_safe_decode(value),
            failure_reason=reason,
            error_message=err,
            retry_count=0,
        )

    # ----------------------------------------------------------- Kapatma

    def _close_consumer(self) -> None:
        if self.consumer:
            self.consumer.close()
        if self.start_time:
            elapsed = time.time() - self.start_time
            rate = self.msg_total / elapsed if elapsed else 0
            logger.info(
                f"🏁 Kafka consumer durdu — {self.msg_total:,} msg • {rate:,.0f} msg/s"
            )
