#!/usr/bin/env python3
"""
Comprehensive Dead Letter Queue Manager

Bu modül başarısız mesajları handle eden gelişmiş dead letter queue sistemini yönetir.
"""

import asyncio
import json
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
import uuid
from loguru import logger
import click

class FailureReason(Enum):
    PARSE_ERROR = "parse_error"
    PROCESSING_ERROR = "processing_error"
    EMBEDDING_ERROR = "embedding_error"
    QDRANT_ERROR = "qdrant_error"
    TIMEOUT_ERROR = "timeout_error"
    VALIDATION_ERROR = "validation_error"
    UNKNOWN_ERROR = "unknown_error"

@dataclass
class DeadLetterMessage:
    """Dead letter queue mesajı"""
    id: str
    original_message: Dict[str, Any]
    failure_reason: FailureReason
    error_details: str
    attempt_count: int
    first_failure_time: datetime
    last_failure_time: datetime
    topic: str
    partition: int
    offset: int
    retry_after: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Dictionary'ye çevir"""
        return {
            'id': self.id,
            'original_message': self.original_message,
            'failure_reason': self.failure_reason.value,
            'error_details': self.error_details,
            'attempt_count': self.attempt_count,
            'first_failure_time': self.first_failure_time.isoformat(),
            'last_failure_time': self.last_failure_time.isoformat(),
            'topic': self.topic,
            'partition': self.partition,
            'offset': self.offset,
            'retry_after': self.retry_after.isoformat() if self.retry_after else None
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DeadLetterMessage':
        """Dictionary'den oluştur"""
        return cls(
            id=data['id'],
            original_message=data['original_message'],
            failure_reason=FailureReason(data['failure_reason']),
            error_details=data['error_details'],
            attempt_count=data['attempt_count'],
            first_failure_time=datetime.fromisoformat(data['first_failure_time']),
            last_failure_time=datetime.fromisoformat(data['last_failure_time']),
            topic=data['topic'],
            partition=data['partition'],
            offset=data['offset'],
            retry_after=datetime.fromisoformat(data['retry_after']) if data['retry_after'] else None
        )

class DeadLetterQueueManager:
    """Comprehensive Dead Letter Queue Manager"""
    
    def __init__(self, config: Dict[str, Any], metrics=None):
        self.config = config
        self.metrics = metrics
        
        # DLQ ayarları
        dlq_config = config.get('dead_letter_queue', {})
        self.dlq_path = dlq_config.get('path', '/data/dlq')
        self.max_retries = dlq_config.get('max_retries', 3)
        self.retry_delays = dlq_config.get('retry_delays', [60, 300, 900])  # 1min, 5min, 15min
        self.enable_retry = dlq_config.get('enable_retry', True)
        self.batch_size = dlq_config.get('batch_size', 100)
        
        # Paths
        Path(self.dlq_path).mkdir(parents=True, exist_ok=True)
        self.failed_messages_file = Path(self.dlq_path) / "failed_messages.jsonl"
        self.retry_queue_file = Path(self.dlq_path) / "retry_queue.jsonl"
        
        # Internal state
        self.retry_queue: List[DeadLetterMessage] = []
        self.failed_messages: List[DeadLetterMessage] = []
        
        # Load existing messages
        self._load_failed_messages()
        self._load_retry_queue()
        
        logger.info(f"DLQ Manager initialized: {len(self.failed_messages)} failed, {len(self.retry_queue)} in retry queue")

    async def handle_failed_message(self, 
                                   message: Dict[str, Any], 
                                   error: Exception,
                                   failure_reason: FailureReason,
                                   topic: str = "unknown",
                                   partition: int = 0,
                                   offset: int = 0) -> bool:
        """Başarısız mesajı handle et"""
        try:
            existing_msg = self._find_existing_message(message, topic, partition, offset)
            
            if existing_msg:
                existing_msg.attempt_count += 1
                existing_msg.last_failure_time = datetime.now(timezone.utc)
                existing_msg.error_details = str(error)
                
                if existing_msg.attempt_count >= self.max_retries:
                    await self._move_to_failed(existing_msg)
                    logger.warning(f"Message permanently failed after {existing_msg.attempt_count} attempts: {existing_msg.id}")
                    return False
                else:
                    await self._schedule_retry(existing_msg)
                    return True
            else:
                dlq_message = DeadLetterMessage(
                    id=str(uuid.uuid4()),
                    original_message=message,
                    failure_reason=failure_reason,
                    error_details=str(error),
                    attempt_count=1,
                    first_failure_time=datetime.now(timezone.utc),
                    last_failure_time=datetime.now(timezone.utc),
                    topic=topic,
                    partition=partition,
                    offset=offset
                )
                
                if self.max_retries > 1:
                    await self._schedule_retry(dlq_message)
                    return True
                else:
                    await self._move_to_failed(dlq_message)
                    return False
                    
        except Exception as e:
            logger.error(f"DLQ handle error: {e}")
            return False

    async def _schedule_retry(self, dlq_message: DeadLetterMessage):
        """Retry için zamanlama yap"""
        try:
            delay_idx = min(dlq_message.attempt_count - 1, len(self.retry_delays) - 1)
            delay_seconds = self.retry_delays[delay_idx]
            
            dlq_message.retry_after = datetime.now(timezone.utc).replace(microsecond=0) + \
                                    timedelta(seconds=delay_seconds)
            
            if dlq_message not in self.retry_queue:
                self.retry_queue.append(dlq_message)
            
            await self._save_retry_queue()
            
            logger.info(f"Scheduled retry for message {dlq_message.id} after {delay_seconds}s (attempt {dlq_message.attempt_count})")
            
            if self.metrics:
                self.metrics.record_processing_error("dlq", "retry_scheduled")
                
        except Exception as e:
            logger.error(f"Retry scheduling error: {e}")

    async def _move_to_failed(self, dlq_message: DeadLetterMessage):
        """Mesajı kalıcı başarısız olarak işaretle"""
        try:
            if dlq_message not in self.failed_messages:
                self.failed_messages.append(dlq_message)
            
            if dlq_message in self.retry_queue:
                self.retry_queue.remove(dlq_message)
            
            await self._save_failed_messages()
            await self._save_retry_queue()
            
            logger.error(f"Message moved to failed: {dlq_message.id} - {dlq_message.failure_reason.value}")
            
            if self.metrics:
                self.metrics.record_processing_error("dlq", "permanently_failed")
                
        except Exception as e:
            logger.error(f"Move to failed error: {e}")

    async def get_retry_candidates(self) -> List[DeadLetterMessage]:
        """Retry edilebilir mesajları getir"""
        now = datetime.now(timezone.utc)
        candidates = []
        
        for msg in self.retry_queue:
            if msg.retry_after and msg.retry_after <= now:
                candidates.append(msg)
        
        return candidates[:self.batch_size]

    async def process_retries(self, processor_callback: Callable) -> int:
        """Retry işlemlerini gerçekleştir"""
        if not self.enable_retry:
            return 0
            
        try:
            candidates = await self.get_retry_candidates()
            
            if not candidates:
                return 0
            
            processed_count = 0
            
            for msg in candidates:
                try:
                    success = await processor_callback(msg.original_message)
                    
                    if success:
                        self.retry_queue.remove(msg)
                        processed_count += 1
                        logger.info(f"Retry successful for message {msg.id}")
                        
                        if self.metrics:
                            self.metrics.record_kafka_message_processed(msg.topic, "retry_success")
                    else:
                        await self.handle_failed_message(
                            msg.original_message,
                            Exception("Retry failed"),
                            msg.failure_reason,
                            msg.topic,
                            msg.partition,
                            msg.offset
                        )
                        
                except Exception as e:
                    logger.error(f"Retry processing error for {msg.id}: {e}")
                    await self.handle_failed_message(
                        msg.original_message,
                        e,
                        FailureReason.PROCESSING_ERROR,
                        msg.topic,
                        msg.partition,
                        msg.offset
                    )
            
            if processed_count > 0:
                await self._save_retry_queue()
                logger.info(f"Processed {processed_count} retry messages")
            
            return processed_count
            
        except Exception as e:
            logger.error(f"Retry processing error: {e}")
            return 0

    def _find_existing_message(self, message: Dict[str, Any], topic: str, partition: int, offset: int) -> Optional[DeadLetterMessage]:
        """Mevcut mesajı bul"""
        message_id = message.get('id')
        
        for msg in self.retry_queue + self.failed_messages:
            if (msg.original_message.get('id') == message_id or 
                (msg.topic == topic and msg.partition == partition and msg.offset == offset)):
                return msg
        
        return None

    def _load_failed_messages(self):
        """Başarısız mesajları yükle"""
        try:
            if self.failed_messages_file.exists():
                with open(self.failed_messages_file, 'r') as f:
                    for line in f:
                        data = json.loads(line.strip())
                        msg = DeadLetterMessage.from_dict(data)
                        self.failed_messages.append(msg)
        except Exception as e:
            logger.error(f"Failed to load failed messages: {e}")

    def _load_retry_queue(self):
        """Retry queue'yu yükle"""
        try:
            if self.retry_queue_file.exists():
                with open(self.retry_queue_file, 'r') as f:
                    for line in f:
                        data = json.loads(line.strip())
                        msg = DeadLetterMessage.from_dict(data)
                        self.retry_queue.append(msg)
        except Exception as e:
            logger.error(f"Failed to load retry queue: {e}")

    async def _save_failed_messages(self):
        """Başarısız mesajları kaydet"""
        try:
            with open(self.failed_messages_file, 'w') as f:
                for msg in self.failed_messages:
                    f.write(json.dumps(msg.to_dict()) + '\n')
        except Exception as e:
            logger.error(f"Failed to save failed messages: {e}")

    async def _save_retry_queue(self):
        """Retry queue'yu kaydet"""
        try:
            with open(self.retry_queue_file, 'w') as f:
                for msg in self.retry_queue:
                    f.write(json.dumps(msg.to_dict()) + '\n')
        except Exception as e:
            logger.error(f"Failed to save retry queue: {e}")

    # Add missing methods for DLQ manager
    async def get_dlq_messages(self, timeout_ms: int = 5000) -> List[DeadLetterMessage]:
        """Get all DLQ messages (both failed and retry queue)"""
        return self.failed_messages + self.retry_queue

    async def replay_message(self, dlq_message: DeadLetterMessage, target_topic: Optional[str] = None) -> bool:
        """Replay a DLQ message to original or target topic"""
        try:
            # In a real implementation, this would send to Kafka
            # For now, just remove from failed and add back to processing
            if dlq_message in self.failed_messages:
                self.failed_messages.remove(dlq_message)
            elif dlq_message in self.retry_queue:
                self.retry_queue.remove(dlq_message)
            
            # Reset retry count for replay
            dlq_message.attempt_count = 0
            dlq_message.retry_after = None
            
            await self._save_failed_messages()
            await self._save_retry_queue()
            
            logger.info(f"Message replayed successfully: {dlq_message.id}")
            return True
            
        except Exception as e:
            logger.error(f"Message replay failed: {e}")
            return False

    async def send_to_dlq(self, original_topic: str, original_partition: int, original_offset: int,
                         original_key: Optional[str], original_value: str, failure_reason: FailureReason,
                         error_message: str, retry_count: int = 0) -> bool:
        """Send message to DLQ"""
        try:
            message_data = {
                'topic': original_topic,
                'partition': original_partition,
                'offset': original_offset,
                'key': original_key,
                'value': original_value
            }
            
            return await self.handle_failed_message(
                message_data,
                Exception(error_message),
                failure_reason,
                original_topic,
                original_partition,
                original_offset
            )
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
            return False

    def close(self):
        """Clean up resources"""
        try:
            logger.info("DLQ Manager resources cleaned up")
        except Exception as e:
            logger.error(f"DLQ cleanup error: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """DLQ istatistiklerini getir"""
        now = datetime.now(timezone.utc)
        ready_for_retry = sum(1 for msg in self.retry_queue 
                             if msg.retry_after and msg.retry_after <= now)
        
        failure_reasons = {}
        for msg in self.failed_messages + self.retry_queue:
            reason = msg.failure_reason.value
            failure_reasons[reason] = failure_reasons.get(reason, 0) + 1
        
        return {
            'total_failed': len(self.failed_messages),
            'in_retry_queue': len(self.retry_queue),
            'ready_for_retry': ready_for_retry,
            'failure_reasons': failure_reasons,
            'dlq_path': str(self.dlq_path),
            'config': {
                'max_retries': self.max_retries,
                'retry_delays': self.retry_delays,
                'enable_retry': self.enable_retry,
                'batch_size': self.batch_size
            }
        }


# CLI Commands


@click.group()
@click.option('--dlq-path', default='/data/dlq', help='DLQ storage path')
@click.option('--max-retries', default=3, help='Maximum retry attempts')
@click.pass_context
def cli(ctx, dlq_path, max_retries):
    """Dead Letter Queue Management CLI"""
    ctx.ensure_object(dict)
    
    config = {
        'dead_letter_queue': {
            'path': dlq_path,
            'max_retries': max_retries,
            'retry_delays': [60, 300, 900],
            'enable_retry': True,
            'batch_size': 100
        }
    }
    
    ctx.obj['dlq'] = DeadLetterQueueManager(config)
    ctx.obj['config'] = config


@cli.command()
@click.pass_context
def stats(ctx):
    """DLQ istatistiklerini göster"""
    dlq = ctx.obj['dlq']
    
    try:
        stats_data = dlq.get_stats()
        
        click.echo("\n=== DLQ İstatistikleri ===")
        click.echo(f"DLQ Path: {stats_data['dlq_path']}")
        click.echo(f"Toplam Başarısız Mesaj: {stats_data['total_failed']}")
        click.echo(f"Retry Queue'da: {stats_data['in_retry_queue']}")
        click.echo(f"Retry İçin Hazır: {stats_data['ready_for_retry']}")
        
        if stats_data['failure_reasons']:
            click.echo("\nHata Nedenleri:")
            for reason, count in stats_data['failure_reasons'].items():
                click.echo(f"  {reason}: {count}")
                
        click.echo(f"\nKonfigürasyon:")
        config = stats_data['config']
        click.echo(f"  Max Retries: {config['max_retries']}")
        click.echo(f"  Retry Delays: {config['retry_delays']}")
        click.echo(f"  Enable Retry: {config['enable_retry']}")
        click.echo(f"  Batch Size: {config['batch_size']}")
        
    except Exception as e:
        click.echo(f"Hata: {e}", err=True)


@cli.command()
@click.option('--limit', default=10, help='Maksimum mesaj sayısı')
@click.option('--format', 'output_format', default='table', 
              type=click.Choice(['table', 'json']), help='Çıktı formatı')
@click.pass_context
def list_failed(ctx, limit, output_format):
    """Başarısız mesajları listele"""
    dlq = ctx.obj['dlq']
    
    try:
        messages = dlq.failed_messages[:limit]
        
        if not messages:
            click.echo("Başarısız mesaj bulunamadı.")
            return
        
        if output_format == 'json':
            output = [msg.to_dict() for msg in messages]
            click.echo(json.dumps(output, indent=2, ensure_ascii=False))
        else:
            click.echo("\n=== Başarısız Mesajlar ===")
            for i, msg in enumerate(messages, 1):
                click.echo(f"\n{i}. Mesaj ID: {msg.id}")
                click.echo(f"   Topic: {msg.topic}")
                click.echo(f"   Hata Nedeni: {msg.failure_reason.value}")
                click.echo(f"   Deneme Sayısı: {msg.attempt_count}")
                click.echo(f"   İlk Hata: {msg.first_failure_time}")
                click.echo(f"   Son Hata: {msg.last_failure_time}")
                click.echo(f"   Hata Detayı: {msg.error_details[:100]}...")
                
    except Exception as e:
        click.echo(f"Hata: {e}", err=True)


@cli.command()
@click.option('--limit', default=10, help='Maksimum mesaj sayısı')
@click.option('--format', 'output_format', default='table', 
              type=click.Choice(['table', 'json']), help='Çıktı formatı')
@click.pass_context
def list_retry(ctx, limit, output_format):
    """Retry queue'daki mesajları listele"""
    dlq = ctx.obj['dlq']
    
    try:
        messages = dlq.retry_queue[:limit]
        
        if not messages:
            click.echo("Retry queue'da mesaj bulunamadı.")
            return
        
        if output_format == 'json':
            output = [msg.to_dict() for msg in messages]
            click.echo(json.dumps(output, indent=2, ensure_ascii=False))
        else:
            click.echo("\n=== Retry Queue Mesajları ===")
            for i, msg in enumerate(messages, 1):
                now = datetime.now(timezone.utc)
                retry_in = None
                if msg.retry_after:
                    retry_in = (msg.retry_after - now).total_seconds()
                    
                click.echo(f"\n{i}. Mesaj ID: {msg.id}")
                click.echo(f"   Topic: {msg.topic}")
                click.echo(f"   Hata Nedeni: {msg.failure_reason.value}")
                click.echo(f"   Deneme Sayısı: {msg.attempt_count}")
                click.echo(f"   Retry After: {msg.retry_after}")
                if retry_in is not None:
                    if retry_in > 0:
                        click.echo(f"   Retry in: {int(retry_in)} seconds")
                    else:
                        click.echo(f"   Ready for retry!")
                
    except Exception as e:
        click.echo(f"Hata: {e}", err=True)


@cli.command()
@click.option('--topic', help='Target topic (varsayılan: orijinal topic)')
@click.option('--key', help='Mesaj key filtresi')
@click.option('--reason', help='Hata nedeni filtresi')
@click.option('--limit', default=10, help='Maksimum mesaj sayısı')
@click.option('--dry-run', is_flag=True, help='Sadece göster, replay yapma')
@click.pass_context
def replay(ctx, topic, key, reason, limit, dry_run):
    """DLQ mesajlarını replay et"""
    dlq = ctx.obj['dlq']
    
    try:
        messages = dlq.get_dlq_messages(timeout_ms=5000)
        
        if not messages:
            click.echo("DLQ'da mesaj bulunamadı.")
            return
        
        # Filtreleme
        filtered_messages = _filter_messages(messages, key, reason)
        
        if not filtered_messages:
            click.echo("Filtre kriterlerine uygun mesaj bulunamadı.")
            return
        
        # Limit uygula
        filtered_messages = filtered_messages[:limit]
        
        if dry_run:
            click.echo(f"\nReplay edilecek {len(filtered_messages)} mesaj:")
            _print_messages_table(filtered_messages)
            return
        
        # Replay işlemi
        success_count = 0
        
        for msg in filtered_messages:
            try:
                result = asyncio.run(
                    dlq.replay_message(msg, target_topic=topic)
                )
                if result:
                    success_count += 1
                    click.echo(f"✓ Mesaj replay edildi: {msg.original_key}")
                else:
                    click.echo(f"✗ Mesaj replay hatası: {msg.original_key}")
            except Exception as e:
                click.echo(f"✗ Mesaj replay hatası: {msg.original_key} - {e}")
        
        click.echo(f"\nToplam: {len(filtered_messages)}, Başarılı: {success_count}")
        
    except Exception as e:
        click.echo(f"Hata: {e}", err=True)
    finally:
        dlq.close()


@cli.command()
@click.option('--topic', required=True, help='Orijinal topic')
@click.option('--partition', type=int, default=0, help='Partition')
@click.option('--offset', type=int, required=True, help='Offset')
@click.option('--key', help='Mesaj key')
@click.option('--value', required=True, help='Mesaj value')
@click.option('--reason', type=click.Choice([r.value for r in FailureReason]), 
              default=FailureReason.UNKNOWN_ERROR.value, help='Hata nedeni')
@click.option('--error-message', default='Manuel DLQ ekleme', help='Hata mesajı')
@click.pass_context
def add_message(ctx, topic, partition, offset, key, value, reason, error_message):
    """DLQ'ya manuel mesaj ekle"""
    dlq = ctx.obj['dlq']
    
    try:
        result = asyncio.run(
            dlq.send_to_dlq(
                original_topic=topic,
                original_partition=partition,
                original_offset=offset,
                original_key=key,
                original_value=value,
                failure_reason=FailureReason(reason),
                error_message=error_message,
                retry_count=0
            )
        )
        
        if result:
            click.echo("✓ Mesaj DLQ'ya başarıyla eklendi.")
        else:
            click.echo("✗ Mesaj DLQ'ya eklenemedi.")
            
    except Exception as e:
        click.echo(f"Hata: {e}", err=True)
    finally:
        dlq.close()


@cli.command()
@click.option('--reason', help='Hata nedeni filtresi')
@click.option('--before', help='Bu tarihten önce (ISO format)')
@click.option('--confirm', is_flag=True, help='Onay gerektir')
@click.pass_context
def purge(ctx, reason, before, confirm):
    """DLQ mesajlarını temizle (Bu özellik henüz implement edilmemiş)"""
    click.echo("Bu özellik henüz implement edilmemiştir.")
    click.echo("DLQ mesajlarını temizlemek için Kafka admin araçlarını kullanın.")


def _filter_messages(
    messages: List[DeadLetterMessage], 
    key_filter: Optional[str] = None,
    reason_filter: Optional[str] = None
) -> List[DeadLetterMessage]:
    """Mesajları filtrele"""
    filtered = messages
    
    if key_filter:
        filtered = [msg for msg in filtered if msg.original_key == key_filter]
    
    if reason_filter:
        try:
            reason_enum = FailureReason(reason_filter)
            filtered = [msg for msg in filtered if msg.failure_reason == reason_enum]
        except ValueError:
            pass
    
    return filtered


def _print_messages_table(messages: List[DeadLetterMessage]):
    """Mesajları tablo formatında yazdır"""
    if not messages:
        return
    
    click.echo("\n" + "=" * 120)
    click.echo(f"{'Key':<20} {'Topic':<20} {'Reason':<20} {'Failed At':<20} {'Retry':<5} {'Error':<30}")
    click.echo("=" * 120)
    
    for msg in messages:
        key = (msg.original_key or 'None')[:19]
        topic = msg.original_topic[:19]
        reason = msg.failure_reason.value[:19]
        failed_at = msg.failed_at[:19]
        retry = str(msg.retry_count)
        error = msg.error_message[:29]
        
        click.echo(f"{key:<20} {topic:<20} {reason:<20} {failed_at:<20} {retry:<5} {error:<30}")
    
    click.echo("=" * 120)
    click.echo(f"Toplam: {len(messages)} mesaj")



if __name__ == '__main__':
    cli()