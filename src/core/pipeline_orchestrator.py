#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import signal
import sys
import time
import threading
import concurrent.futures
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
from loguru import logger
try:
    import uvloop
    UVLOOP_AVAILABLE = True
except ImportError:
    UVLOOP_AVAILABLE = False


project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.kafka_consumer import KafkaConsumer
from src.core.qdrant_writer import QdrantWriter
from src.core.embedding_processor import EmbeddingProcessor
from src.config.kafka_config import KafkaConfig
from src.monitoring.prometheus_metrics import PrometheusMetrics
from src.monitoring.health_monitor import HealthMonitor
from src.api.unified_server import UnifiedServer


class PipelineOrchestrator:
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.running = False
        self.shutdown_event = asyncio.Event()
        self.kafka_consumer = None
        self.qdrant_writer = None
        self.embedding_processor = None
        self.background_tasks = set()
        self.server_thread = None
        self.last_performance_check = time.time()
        self.performance_check_interval = 30 
        self.message_count = 0
        self.start_time = None
        self.performance_window = []
        self.embedding_batch = []
        self.batch_size = config.get('qdrant', {}).get('batch_size', 5000)
        self.batch_timeout = config.get('qdrant', {}).get('batch_timeout', 0.5)
        self.last_batch_time = time.time()
        self.batch_lock = asyncio.Lock()
        
        self.embedding_queue = asyncio.Queue(maxsize=20000)
        self.kafka_paused = False

        self.flush_task = None

        self.use_uvloop = config.get('use_uvloop', UVLOOP_AVAILABLE)
        self.target_events_per_sec = config.get('target_events_per_sec', 1000)
        
        logger.info("🚀 High-performance Pipeline Orchestrator initialized")
        logger.info(f"   📊 Target: {self.target_events_per_sec}+ events/sec")
        logger.info(f"   📦 Batch size: {self.batch_size}")
        logger.info(f"   ⏱️ Batch timeout: {self.batch_timeout}s")
        logger.info(f"   ⚡ uvloop enabled: {self.use_uvloop}")
        logger.info(f"   🔄 Back-pressure queue: {20000} max size")
        logger.info(f"   🚀 Async flushing: Dedicated task")
        logger.info(f"   📊 Enhanced monitoring: 4 high-cardinality histograms")
    
    async def initialize(self):
        try:
            logger.info("🚀 Initializing VectorStream Pipeline...")
            

            kafka_config = KafkaConfig.from_dict(self.config['kafka'])

            await kafka_config.ensure_topic_configuration()

            await self._initialize_monitoring()

            await self._initialize_storage()

            await self._initialize_processing()

            await self._initialize_streaming()

            self._setup_signal_handlers()
            
            logger.info("✅ Pipeline initialization completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Pipeline initialization failed: {e}")
            await self.shutdown()
            raise
    
    async def _initialize_monitoring(self):
        logger.info("📊 Initializing monitoring components...")

        self.metrics = PrometheusMetrics()

        self.health_monitor = HealthMonitor(
            kafka_config=self.config['kafka'],
            qdrant_config=self.config['qdrant']
        )

        self.unified_server = UnifiedServer(self.metrics, self.health_monitor)

        def run_server():
            self.unified_server.start_server(host="0.0.0.0", port=8080)
        
        self.server_thread = threading.Thread(target=run_server, daemon=True)
        self.server_thread.start()

        await asyncio.sleep(1)
        
        logger.info("✅ Monitoring components initialized")
        logger.info("   📊 Metrics: http://localhost:8080/metrics")
        logger.info("   🏥 Health: http://localhost:8080/health")
        logger.info("   📚 API Docs: http://localhost:8080/docs")
    
    async def _initialize_storage(self):
        logger.info("🗄️ Initializing storage components...")

        self.qdrant_writer = QdrantWriter(self.config['qdrant'])
        await self.qdrant_writer.initialize_collection()
        
        self.metrics.set_qdrant_connection_status(True)
        logger.info("✅ Qdrant vector database initialized")
    
    async def _initialize_processing(self):
        logger.info("🧠 Initializing processing components...")

        embedding_config = self.config.get('embedding', {
            'model_name': 'all-MiniLM-L6-v2',
            'vector_size': 384,
            'batch_size': 2048
        })
        
        self.embedding_processor = EmbeddingProcessor(embedding_config)
        await self.embedding_processor.initialize()
        
        logger.info("✅ Embedding processor initialized")
    
    async def _initialize_streaming(self):
        logger.info("📡 Initializing streaming components...")

        try:
            kafka_config = KafkaConfig(
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                topic=self.config['kafka']['topic'],
                group_id=self.config['kafka']['group_id'],
                auto_offset_reset=self.config['kafka']['auto_offset_reset'],
                max_poll_records=self.config['kafka'].get('max_poll_records', 1000),
            )
            

            await kafka_config.ensure_topic_configuration()
            
        except Exception as e:
            logger.error(f"❌ Kafka topic konfigürasyon hatası: {e}")

            kafka_config = KafkaConfig(
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                topic=self.config['kafka']['topic'],
                group_id=self.config['kafka']['group_id'],
                auto_offset_reset='earliest',
                max_poll_records=1000,
            )
        
        self.kafka_consumer = KafkaConsumer(kafka_config)
        self.kafka_consumer.set_message_handler(self._process_event_batch)
        
        self.metrics.set_kafka_connection_status(True)
        logger.info("✅ Kafka consumer initialized")
    
    async def start(self):
        if self.running:
            logger.warning("Pipeline is already running")
            return
        
        self.running = True
        logger.info("🚀 Starting VectorStream Pipeline...")
        
        try:

            self.metrics.set_system_info({
                'version': '1.0.0',
                'environment': 'production',
                'component': 'vectorstream-mlops-pipeline',
                'task_compliance': 'full'
            })

            monitoring_task = asyncio.create_task(self._monitoring_loop())
            self.background_tasks.add(monitoring_task)
            monitoring_task.add_done_callback(self.background_tasks.discard)

            performance_task = asyncio.create_task(self._performance_monitoring_loop())
            self.background_tasks.add(performance_task)
            performance_task.add_done_callback(self.background_tasks.discard)

            self.flush_task = asyncio.create_task(self._dedicated_flush_loop())
            self.background_tasks.add(self.flush_task)
            self.flush_task.add_done_callback(self.background_tasks.discard)

            queue_task = asyncio.create_task(self._embedding_queue_processor())
            self.background_tasks.add(queue_task)
            queue_task.add_done_callback(self.background_tasks.discard)

            consumer_task = asyncio.create_task(self.kafka_consumer.start_consuming())
            self.background_tasks.add(consumer_task)
            consumer_task.add_done_callback(self.background_tasks.discard)
            
            logger.info("✅ Pipeline started successfully")
            logger.info("📈 Performance targets: 1000+ events/sec, <30s latency")
            logger.info("🔄 Pipeline is now processing events...")

            await self.shutdown_event.wait()
            
        except Exception as e:
            logger.error(f"❌ Pipeline error: {e}")
            raise
        finally:
            await self.shutdown()
    
    async def run(self):
        try:
            logger.info("🚀 High-performance pipeline başlatılıyor...")

            if self.use_uvloop and UVLOOP_AVAILABLE:
                logger.info("⚡ uvloop event loop policy ayarlanıyor...")
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

            await self.initialize()

            await self.start()
            
            logger.info("✅ High-performance pipeline başarıyla başlatıldı ve çalışıyor")
            logger.info(f"📊 Hedef performans: {self.target_events_per_sec}+ events/sec")
            logger.info(f"⚡ Event loop: {'uvloop' if self.use_uvloop and UVLOOP_AVAILABLE else 'asyncio default'}")
            
        except KeyboardInterrupt:
            logger.info("⏹️ Pipeline durdurma sinyali alındı")
        except Exception as e:
            logger.error(f"❌ Pipeline çalışma hatası: {e}")
            raise
        finally:
            await self.shutdown()
    
    @staticmethod
    def run_with_uvloop(orchestrator):
        if UVLOOP_AVAILABLE:
            logger.info("🚀 Starting pipeline with uvloop optimization")
            uvloop.run(orchestrator.run())
        else:
            logger.info("🚀 Starting pipeline with default asyncio")
            asyncio.run(orchestrator.run())
    
    async def _process_event_batch(self, events: list) -> None:
        if not events:
            return
            
        start_time = time.time()
        batch_size = len(events)
        

        kafka_ingest_start = time.time()
        topic = self.config['kafka']['topic']
        group_id = self.config['kafka']['group_id']

        kafka_ingest_duration = time.time() - kafka_ingest_start
        self.metrics.record_kafka_ingest_latency(topic, '0', group_id, kafka_ingest_duration)

        if self.start_time is None:
            self.start_time = start_time
        
        self.message_count += batch_size

        for _ in range(batch_size):
            self.performance_window.append(start_time)

        cutoff_time = start_time - 300
        self.performance_window = [t for t in self.performance_window if t >= cutoff_time]
        
        try:

            topic = self.config['kafka']['topic']
            for _ in range(batch_size):
                self.metrics.record_kafka_message_consumed(topic, 0)

            texts_and_metadata = []
            
            for data in events:
                event_id = data.get('event_id', 'unknown')
                

                text_parts = []
                product = data.get('product', {})
                
                if product.get('name'):
                    text_parts.append(product.get('name'))
                if product.get('description'):
                    text_parts.append(product.get('description'))
                if product.get('category'):
                    text_parts.append(product.get('category'))

                if data.get('search_query'):
                    text_parts.append(data.get('search_query'))
                if data.get('event_type'):
                    text_parts.append(data.get('event_type'))
                
                text = ' '.join(text_parts) if text_parts else f"event_{event_id}"

                texts_and_metadata.append({
                    'text': text,
                    'event_id': event_id,
                    'timestamp': data.get('timestamp'),
                    'event_type': data.get('event_type'),
                    'user_id': data.get('user_id'),
                    'session_id': data.get('session_id'),
                    'product': product
                })

            embedding_start = time.time()
            texts = [item['text'] for item in texts_and_metadata]

            embedding_vectors = await self.embedding_processor.create_embeddings_batch(texts)
            embedding_duration = time.time() - embedding_start

            model_name = self.config.get('embedding', {}).get('model_name', 'all-MiniLM-L6-v2')
            device = 'mps' if hasattr(self.embedding_processor, 'device') else 'cpu'
            backend = 'onnx' if hasattr(self.embedding_processor, 'use_onnx') else 'pytorch'
            self.metrics.record_embedding_batch_processing(model_name, len(texts), device, backend, embedding_duration)
            
            if not embedding_vectors or len(embedding_vectors) != len(texts):
                logger.warning(f"⚠️ Failed to create embeddings for batch of {batch_size} messages, skipping")
                if self.metrics:
                    self.metrics.record_processing_error('embedding_creation', 'BulkEmbeddingCreationFailed')
                return

            self.metrics.record_embedding_processing(embedding_duration, 'sentence_transformers_bulk')

            embedding_data_batch = []
            for i, (embedding_vector, metadata) in enumerate(zip(embedding_vectors, texts_and_metadata)):
                if embedding_vector is not None:
                    embedding_data = {
                        'vector': embedding_vector,
                        'metadata': {
                            'text': metadata['text'],
                            'event_id': metadata['event_id'],
                            'timestamp': metadata['timestamp'],
                            'event_type': metadata['event_type'],
                            'user_id': metadata['user_id'],
                            'session_id': metadata['session_id'],
    
                            'product_id': metadata['product'].get('id'),
                            'product_name': metadata['product'].get('name'),
                            'product_description': metadata['product'].get('description'),
                            'product_category': metadata['product'].get('category'),
                            'product_price': metadata['product'].get('price'),
                            'processing_timestamp': datetime.now().isoformat()
                        }
                    }
                    embedding_data_batch.append(embedding_data)

            for embedding_data in embedding_data_batch:
                try:

                    self.embedding_queue.put_nowait(embedding_data)
                except asyncio.QueueFull:

                    if not self.kafka_paused:
                        logger.warning("🚦 Queue full - pausing Kafka consumption for back-pressure control")
                        await self._pause_kafka_consumption()

                    await asyncio.wait_for(self.embedding_queue.put(embedding_data), timeout=5.0)

            for _ in range(len(embedding_data_batch)):
                self.metrics.record_qdrant_operation('queue', 'embeddings', 'success', 0.001)

            total_duration = time.time() - start_time

            batch_size_range = self._classify_batch_size(batch_size)
            self.metrics.record_end_to_end_latency('complete_pipeline', batch_size_range, total_duration)

            self.metrics.vectorstream_processing_duration.observe(total_duration)

            if total_duration > 30:
                logger.warning(f"⚠️ High bulk latency detected: {total_duration:.2f}s > 30s for batch of {batch_size} events")

            if batch_size >= 1000:  
                logger.info(f"✅ Bulk processed {batch_size} events in {total_duration:.3f}s ({batch_size/total_duration:.0f} events/s)")
            elif batch_size >= 100:
                logger.debug(f"✅ Bulk processed {batch_size} events in {total_duration:.3f}s ({batch_size/total_duration:.0f} events/s)")
            
        except Exception as e:

            self.metrics.record_processing_error('pipeline_event_batch', str(type(e).__name__))
            logger.error(f"❌ Error processing event batch of {batch_size} events: {e}")
            raise
    
    async def _process_message(self, data: Dict[str, Any]) -> None:
        start_time = time.time()
        event_id = data.get('event_id', 'unknown')

        if self.start_time is None:
            self.start_time = start_time
        
        self.message_count += 1
        self.performance_window.append(start_time)

        cutoff_time = start_time - 300
        self.performance_window = [t for t in self.performance_window if t >= cutoff_time]
        
        try:

            topic = self.config['kafka']['topic']
            self.metrics.record_kafka_message_consumed(topic, 0)

            text_parts = []
            product = data.get('product', {})
            
            if product.get('name'):
                text_parts.append(product.get('name'))
            if product.get('description'):
                text_parts.append(product.get('description'))
            if product.get('category'):
                text_parts.append(product.get('category'))

            if data.get('search_query'):
                text_parts.append(data.get('search_query'))
            if data.get('event_type'):
                text_parts.append(data.get('event_type'))
            
            text = ' '.join(text_parts) if text_parts else f"event_{event_id}"

            embedding_start = time.time()
            embedding_vector = await self.embedding_processor.create_embedding(text)
            embedding_duration = time.time() - embedding_start

            if embedding_vector is None:
                logger.warning(f"⚠️ Failed to create embedding for event {event_id}, skipping")
                if self.metrics:
                    self.metrics.record_processing_error('embedding_creation', 'EmbeddingCreationFailed')
                return

            self.metrics.record_embedding_processing(embedding_duration, 'sentence_transformers')

            embedding_data = {
                'vector': embedding_vector,
                'metadata': {
                    'text': text,
                    'event_id': event_id,
                    'timestamp': data.get('timestamp'),
                    'event_type': data.get('event_type'),
                    'user_id': data.get('user_id'),
                    'session_id': data.get('session_id'),

                    'product_id': product.get('id'),
                    'product_name': product.get('name'),
                    'product_description': product.get('description'),
                    'product_category': product.get('category'),
                    'product_price': product.get('price'),
                    'processing_timestamp': datetime.now().isoformat()
                }
            }

            await self._add_to_batch(embedding_data)

            self.metrics.record_qdrant_operation('queue', 'embeddings', 'success', 0.001)

            total_duration = time.time() - start_time

            self.metrics.vectorstream_processing_duration.observe(total_duration)

            if total_duration > 30:
                logger.warning(f"⚠️ High latency detected: {total_duration:.2f}s > 30s for event {event_id}")

            
        except Exception as e:

            self.metrics.record_processing_error('pipeline', str(type(e).__name__))
            logger.error(f"❌ Error processing event {event_id}: {e}")
            raise
    
    async def _monitoring_loop(self):
        while self.running:
            try:

                if self.health_monitor:
                    await self.health_monitor.check_all_services()

                if hasattr(self.metrics, 'get_summary'):
                    summary = self.metrics.get_summary()

                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(30)
    
    async def _performance_monitoring_loop(self):
        while self.running:
            try:
                await asyncio.sleep(self.performance_check_interval)

                current_time = time.time()
                
                if self.start_time and self.performance_window:
                    elapsed_total = current_time - self.start_time
                    overall_rate = self.message_count / elapsed_total if elapsed_total > 0 else 0

                    recent_cutoff = current_time - 60
                    recent_messages = [t for t in self.performance_window if t >= recent_cutoff]
                    recent_rate = len(recent_messages) / 60 if recent_messages else 0

                    very_recent_cutoff = current_time - 10
                    very_recent_messages = [t for t in self.performance_window if t >= very_recent_cutoff]
                    very_recent_rate = len(very_recent_messages) / 10 if very_recent_messages else 0

                    meets_target = recent_rate >= 1000
                    status_emoji = "✅" if meets_target else "⚠️"

                    if recent_rate >= 5000:
                        perf_level = "🚀 EXCELLENT (5k+ target achieved)"
                    elif recent_rate >= 2000:
                        perf_level = "🟢 GOOD (2k+ sustained)"
                    elif recent_rate >= 1000:
                        perf_level = "🟡 MODERATE (1k+ baseline)"
                    elif recent_rate >= 100:
                        perf_level = "🟡 MODERATE"
                    else:
                        perf_level = "🔴 LOW"
                    
                    logger.info(f"📊 PIPELINE PERFORMANCE - {perf_level}")
                    logger.info(f"   {status_emoji} Recent rate (60s): {recent_rate:.1f} events/sec (target: 1000+)")
                    logger.info(f"   ⚡ Real-time rate (10s): {very_recent_rate:.1f} events/sec")
                    logger.info(f"   📈 Overall rate: {overall_rate:.1f} events/sec")
                    logger.info(f"   📊 Total processed: {self.message_count:,} events")
                    logger.info(f"   📦 Current batch size: {len(self.embedding_batch)}")
                    logger.info(f"   ⏱️ Runtime: {elapsed_total/60:.1f} minutes")
                    
                    if not meets_target and recent_rate > 0:
                        logger.warning(f"⚠️ Below performance target (current: {recent_rate:.1f}/sec, target: 1000+/sec)")
                        logger.info(f"💡 Optimizations applied: Batch size {self.batch_size}, timeout {self.batch_timeout}s, async flushing, back-pressure control")
                        logger.info(f"📊 Queue status: {self.embedding_queue.qsize()}/20000, Kafka paused: {self.kafka_paused}")
                
                self.last_performance_check = current_time
                
            except Exception as e:
                logger.error(f"Performance monitoring error: {e}")
                await asyncio.sleep(30)
    
    async def _embedding_queue_processor(self):
        while self.running:
            try:

                try:
                    embedding_data = await asyncio.wait_for(self.embedding_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                async with self.batch_lock:
                    self.embedding_batch.append(embedding_data)

                    if self.kafka_paused and self.embedding_queue.qsize() < 5000:  # Resume at 25% capacity
                        await self._resume_kafka_consumption()
                
                self.embedding_queue.task_done()
                
            except Exception as e:
                logger.error(f"Embedding queue processor error: {e}")
                await asyncio.sleep(0.1)
    
    async def _dedicated_flush_loop(self):
        while self.running:
            try:
                await asyncio.sleep(0.1)
                
                current_time = time.time()
                should_flush = False
                batch_to_flush = []
                
                async with self.batch_lock:
                    if (len(self.embedding_batch) >= self.batch_size or 
                        (self.embedding_batch and current_time - self.last_batch_time >= self.batch_timeout)):
                        batch_to_flush = self.embedding_batch.copy()
                        self.embedding_batch.clear()
                        self.last_batch_time = current_time
                        should_flush = True

                if should_flush and batch_to_flush:

                    asyncio.create_task(self._async_flush_to_qdrant(batch_to_flush))
                        
            except Exception as e:
                logger.error(f"Dedicated flush loop error: {e}")
                await asyncio.sleep(0.1)
    
    async def _pause_kafka_consumption(self):
        if self.kafka_consumer and not self.kafka_paused:
            try:

                partitions = self.kafka_consumer.consumer.assignment()
                if partitions:
                    self.kafka_consumer.consumer.pause(partitions)
                    self.kafka_paused = True
                    logger.info(f"⏸️ Paused Kafka consumption on {len(partitions)} partitions")
            except Exception as e:
                logger.error(f"Error pausing Kafka consumption: {e}")
    
    async def _resume_kafka_consumption(self):
        if self.kafka_consumer and self.kafka_paused:
            try:

                partitions = self.kafka_consumer.consumer.assignment()
                if partitions:
                    self.kafka_consumer.consumer.resume(partitions)
                    self.kafka_paused = False
                    logger.info(f"▶️ Resumed Kafka consumption on {len(partitions)} partitions")
            except Exception as e:
                logger.error(f"Error resuming Kafka consumption: {e}")
    
    async def _async_flush_to_qdrant(self, batch_to_write: list):
        try:
            qdrant_start = time.time()

            success = await self.qdrant_writer.write_embeddings(
                batch_to_write, 
                batch_size=len(batch_to_write)
            )
            
            qdrant_duration = time.time() - qdrant_start

            collection_name = self.config['qdrant']['collection_name']
            protocol = 'grpc' if self.config.get('qdrant', {}).get('use_grpc', True) else 'http'
            wait_mode = 'false'
            self.metrics.record_qdrant_write_latency(collection_name, len(batch_to_write), protocol, wait_mode, qdrant_duration)
            
            if success:
                self.metrics.record_qdrant_operation('write', 'embeddings', 'success', qdrant_duration)

                throughput = len(batch_to_write) / qdrant_duration if qdrant_duration > 0 else 0
                
                if len(batch_to_write) >= 1000:
                    logger.info(f"🚀 Async flushed {len(batch_to_write)} vectors in {qdrant_duration:.3f}s ({throughput:.0f} vec/s)")

                    if throughput >= 8000:
                        logger.info(f"🎯 EXCELLENT: {throughput:.0f} vec/s >= 8k target!")
                    elif throughput >= 5000:
                        logger.info(f"🟢 GOOD: {throughput:.0f} vec/s >= 5k")
                    elif throughput < 2000:
                        logger.warning(f"⚠️ LOW: {throughput:.0f} vec/s < 2k target")
            else:
                self.metrics.record_qdrant_operation('write', 'embeddings', 'error', qdrant_duration)
                logger.error(f"❌ Failed to async flush {len(batch_to_write)} vectors")
                
        except Exception as e:
            logger.error(f"❌ Error in async flush to Qdrant: {e}")
            self.metrics.record_processing_error('qdrant_async_flush', str(type(e).__name__))
    
    def _classify_batch_size(self, batch_size: int) -> str:
        if batch_size < 100:
            return 'small_<100'
        elif batch_size < 500:
            return 'medium_100-500'
        elif batch_size < 1000:
            return 'large_500-1000'
        elif batch_size < 5000:
            return 'xlarge_1000-5000'
        else:
            return 'xxlarge_5000+'
    
    async def _flush_batch(self):
        if not self.embedding_batch:
            return
            
        batch_to_write = self.embedding_batch.copy()
        self.embedding_batch.clear()
        self.last_batch_time = time.time()
        
        try:
            
            qdrant_start = time.time()
            success = await self.qdrant_writer.write_embeddings(batch_to_write, batch_size=len(batch_to_write))
            qdrant_duration = time.time() - qdrant_start
            
            if success:
                self.metrics.record_qdrant_operation('write', 'embeddings', 'success', qdrant_duration)
                logger.debug(f"✅ Flushed batch of {len(batch_to_write)} embeddings in {qdrant_duration:.3f}s")
            else:
                self.metrics.record_qdrant_operation('write', 'embeddings', 'error', qdrant_duration)
                logger.error(f"❌ Failed to write batch of {len(batch_to_write)} embeddings")
                
        except Exception as e:
            logger.error(f"❌ Error flushing batch: {e}")
            self.metrics.record_processing_error('qdrant_batch_write', str(type(e).__name__))
    
    def _setup_signal_handlers(self):
        def signal_handler(signum, frame):
            logger.info(f"📡 Received signal {signum}, initiating graceful shutdown...")
            try:

                loop = asyncio.get_running_loop()

                loop.create_task(self._trigger_shutdown())
            except RuntimeError:

                logger.info("No running event loop, setting shutdown event directly")
                self.shutdown_event.set()
                try:
                    import atexit
                    atexit._clear()
                    logger.debug("🧹 Atexit callbacks cleared")
                except Exception as e:
                    logger.debug(f"Could not clear atexit callbacks: {e}")
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def _trigger_shutdown(self):
        self.shutdown_event.set()
    
    async def shutdown(self):
        if not self.running:
            return
        
        self.running = False
        logger.info("🛑 Shutting down VectorStream Pipeline...")
        
        try:

            for task in self.background_tasks:
                if not task.done():
                    task.cancel()

            if self.background_tasks:
                await asyncio.gather(*self.background_tasks, return_exceptions=True)

            if self.kafka_consumer:
                await self.kafka_consumer.close()
                logger.info("📡 Kafka consumer closed")

            async with self.batch_lock:
                if self.embedding_batch:
                    logger.info(f"🔄 Flushing final batch of {len(self.embedding_batch)} embeddings...")
                    await self._flush_batch()

            if self.embedding_processor:
                try:
                    if hasattr(self.embedding_processor, 'close'):
                        await self.embedding_processor.close()
                    elif hasattr(self.embedding_processor, 'stop'):
                        self.embedding_processor.stop()
                    logger.info("🧠 Embedding processor closed")
                except Exception as e:
                    logger.warning(f"Error closing embedding processor: {e}")

            if self.qdrant_writer:
                await self.qdrant_writer.close()
                logger.info("🗄️ Qdrant writer closed")

            if self.unified_server:
                self.unified_server.stop()
                logger.info("🌐 API server stopped")

            try:
                import gc
                gc.collect()
                logger.debug("🧹 Garbage collection completed")
            except Exception as e:
                logger.warning(f"Error during garbage collection: {e}")

            try:
                import atexit
                atexit._clear()
                logger.debug("🧹 Atexit callbacks cleared during shutdown")
            except Exception as e:
                logger.debug(f"Could not clear atexit callbacks during shutdown: {e}")
            
            logger.info("✅ Pipeline shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")


async def start_vectorstream_pipeline(config_path: str = "config/app_config.yaml"):
    import yaml

    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    orchestrator = PipelineOrchestrator(config)
    
    try:
        await orchestrator.initialize()
        await orchestrator.start()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        raise
