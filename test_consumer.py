#!/usr/bin/env python3
"""
Simple consumer test for VectorStream Pipeline
"""

import asyncio
import sys
import json
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.kafka_consumer import KafkaConsumer
from src.config.kafka_config import KafkaConfig

def test_message_handler(message):
    """Simple test message handler"""
    print(f"📨 Received message: {message}")
    return message

async def test_consumer():
    """Test consumer functionality"""
    # Consumer config
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        topic="ecommerce-events",
        group_id="test-consumer",
        auto_offset_reset="latest"
    )
    
    # Create consumer
    consumer = KafkaConsumer(kafka_config)
    consumer.set_message_handler(test_message_handler)
    
    print("🚀 Starting consumer test...")
    print("📡 Waiting for messages...")
    
    try:
        await consumer.start_consuming()
    except KeyboardInterrupt:
        print("\n🛑 Consumer test stopped by user")
    except Exception as e:
        print(f"❌ Consumer test failed: {e}")
    finally:
        await consumer.close()

if __name__ == "__main__":
    asyncio.run(test_consumer())
