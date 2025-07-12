"""Dead Letter Queue management CLI tool."""

import asyncio
import json
import click
from datetime import datetime
from typing import List, Optional

from src.utils.dead_letter_queue import (
    DeadLetterQueue,
    DeadLetterMessage,
    DLQConfig,
    FailureReason,
    get_dlq
)


@click.group()
@click.option('--dlq-topic', default='dead-letter-queue', help='DLQ topic name')
@click.option('--kafka-servers', default='localhost:9092', help='Kafka bootstrap servers')
@click.pass_context
def cli(ctx, dlq_topic, kafka_servers):
    """Dead Letter Queue Management CLI"""
    ctx.ensure_object(dict)
    
    config = DLQConfig(
        dlq_topic=dlq_topic,
        kafka_bootstrap_servers=kafka_servers
    )
    
    ctx.obj['dlq'] = DeadLetterQueue(config)
    ctx.obj['config'] = config


@cli.command()
@click.pass_context
def stats(ctx):
    """DLQ istatistiklerini göster"""
    dlq = ctx.obj['dlq']
    
    try:
        stats_data = dlq.get_dlq_stats()
        
        click.echo("\n=== DLQ İstatistikleri ===")
        click.echo(f"Topic: {ctx.obj['config'].dlq_topic}")
        click.echo(f"Toplam Mesaj: {stats_data.get('total_messages', 0)}")
        
        if 'partitions' in stats_data:
            click.echo("\nPartition Detayları:")
            for partition, data in stats_data['partitions'].items():
                click.echo(f"  Partition {partition}:")
                click.echo(f"    High Water Mark: {data.get('high_water_mark', 'N/A')}")
                click.echo(f"    Low Water Mark: {data.get('low_water_mark', 'N/A')}")
                click.echo(f"    Lag: {data.get('lag', 'N/A')}")
        
        if 'failure_reasons' in stats_data:
            click.echo("\nHata Nedenleri:")
            for reason, count in stats_data['failure_reasons'].items():
                click.echo(f"  {reason}: {count}")
                
    except Exception as e:
        click.echo(f"Hata: {e}", err=True)
    finally:
        dlq.close()


@cli.command()
@click.option('--limit', default=10, help='Maksimum mesaj sayısı')
@click.option('--timeout', default=5000, help='Timeout (ms)')
@click.option('--format', 'output_format', default='table', 
              type=click.Choice(['table', 'json']), help='Çıktı formatı')
@click.pass_context
def list_messages(ctx, limit, timeout, output_format):
    """DLQ mesajlarını listele"""
    dlq = ctx.obj['dlq']
    
    try:
        messages = dlq.get_dlq_messages(timeout_ms=timeout)
        
        if not messages:
            click.echo("DLQ'da mesaj bulunamadı.")
            return
        
        # Limit uygula
        messages = messages[:limit]
        
        if output_format == 'json':
            output = [msg.to_dict() for msg in messages]
            click.echo(json.dumps(output, indent=2, ensure_ascii=False))
        else:
            _print_messages_table(messages)
            
    except Exception as e:
        click.echo(f"Hata: {e}", err=True)
    finally:
        dlq.close()


@cli.command()
@click.option('--topic', help='Hedef topic (varsayılan: orijinal topic)')
@click.option('--key', help='Mesaj key filtresi')
@click.option('--reason', help='Hata nedeni filtresi')
@click.option('--dry-run', is_flag=True, help='Sadece göster, replay yapma')
@click.option('--limit', default=10, help='Maksimum replay sayısı')
@click.pass_context
def replay(ctx, topic, key, reason, dry_run, limit):
    """DLQ mesajlarını tekrar oynat"""
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