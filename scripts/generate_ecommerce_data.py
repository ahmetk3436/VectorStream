#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VectorStream: Real-time E-Commerce Behavior Analysis Pipeline
Event Data Generator - Generates events according to MLOps task requirements
"""

import json
import random
import time
import sys
import uuid
import numpy as np
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any
from kafka import KafkaProducer
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

class ECommerceDataGenerator:
    """
    E-commerce event data generator for VectorStream pipeline
    
    Generates events with exact structure required by MLOps task:
    {
        "event_id": "uuid",
        "timestamp": "2024-01-15T10:30:00Z", 
        "user_id": "user123",
        "event_type": "purchase",
        "product": {
            "id": "uuid",
            "name": "Ürün Adı",
            "description": "Detaylı ürün açıklaması...",
            "category": "Elektronik", 
            "price": 1299.99
        },
        "session_id": "session789"
    }
    """
    
    def __init__(self):
        self.products = [
            {
                "id": "prod_001",
                "name": "iPhone 15 Pro",
                "description": "Apple iPhone 15 Pro 256GB Doğal Titanyum, A17 Pro çip, ProRes video kaydı, Titanium tasarım, Dynamic Island, Always-On display",
                "category": "Elektronik",
                "price": 45000.00
            },
            {
                "id": "prod_002", 
                "name": "Samsung Galaxy S24 Ultra",
                "description": "Samsung Galaxy S24 Ultra 512GB Phantom Black, S Pen dahil, 200MP kamera, AI destekli fotoğraf çekimi, Snapdragon 8 Gen 3",
                "category": "Elektronik",
                "price": 35000.00
            },
            {
                "id": "prod_003",
                "name": "MacBook Air M3",
                "description": "Apple MacBook Air 13 inç M3 çip 8GB RAM 256GB SSD, Liquid Retina display, 18 saat pil ömrü, sessiz çalışma",
                "category": "Bilgisayar",
                "price": 55000.00
            },
            {
                "id": "prod_004",
                "name": "Nike Air Max 270",
                "description": "Nike Air Max 270 Erkek Spor Ayakkabı Siyah, Air Max teknolojisi, nefes alabilir mesh kumaş, koşu için ideal",
                "category": "Ayakkabı",
                "price": 3500.00
            },
            {
                "id": "prod_005",
                "name": "Adidas Ultraboost 22",
                "description": "Adidas Ultraboost 22 Koşu Ayakkabısı Beyaz, Boost teknolojisi, enerji geri dönüşü, PrimeKnit üst yapı",
                "category": "Ayakkabı",
                "price": 4200.00
            },
            {
                "id": "prod_006",
                "name": "Sony WH-1000XM5",
                "description": "Sony WH-1000XM5 Kablosuz Gürültü Önleyici Kulaklık, 30 saat pil ömrü, Hi-Res Audio, adaptive sound control",
                "category": "Elektronik",
                "price": 8500.00
            },
            {
                "id": "prod_007",
                "name": "Zara Erkek Gömlek",
                "description": "Zara Erkek Slim Fit Pamuklu Gömlek Beyaz, %100 pamuk, kolay ütüleme özelliği, modern kesim",
                "category": "Giyim",
                "price": 450.00
            },
            {
                "id": "prod_008",
                "name": "H&M Kadın Elbise",
                "description": "H&M Kadın Midi Elbise Lacivert Çiçek Desenli, polyester karışımı, kolayca yıkanabilir, günlük kullanım",
                "category": "Giyim",
                "price": 320.00
            },
            {
                "id": "prod_009",
                "name": "IKEA LINNMON Masa",
                "description": "IKEA LINNMON Çalışma Masası 120x60 cm Beyaz, melamin yüzey, kolay montaj, modern ofis tasarımı",
                "category": "Mobilya",
                "price": 1200.00
            },
            {
                "id": "prod_010",
                "name": "Python Programlama Kitabı",
                "description": "Python ile Veri Bilimi ve Makine Öğrenmesi kitabı, 500 sayfa, pratik örnekler, başlangıçtan ileri seviyeye",
                "category": "Kitap",
                "price": 85.00
            }
        ]
        
        self.event_types = [
            "view",            
            "add_to_cart",     
            "remove_from_cart",
            "purchase",      
            "wishlist_add",   
            "search"        
        ]
        
        self.users = [f"user{i:03d}" for i in range(1, 1001)]  # user001 to user1000
        
        self.sessions = [f"session{i:04d}" for i in range(1, 10001)]  # session0001 to session10000
        
        self.payment_methods = ["credit_card", "debit_card", "paypal", "bank_transfer", "cash_on_delivery"]
        
        self.sources = ["web", "mobile_app", "tablet"]
        
    def generate_event(self) -> Dict[str, Any]:
        """
        Generate event with exact structure required by MLOps task
        
        Task event structure:
        {
            "event_id": "uuid",
            "timestamp": "2024-01-15T10:30:00Z",
            "user_id": "user123", 
            "event_type": "purchase",
            "product": {
                "id": "uuid",
                "name": "Ürün Adı",
                "description": "Detaylı ürün açıklaması...",
                "category": "Elektronik",
                "price": 1299.99
            },
            "session_id": "session789"
        }
        """
        event_type = random.choice(self.event_types)
        product = random.choice(self.products)
        
        # Base event structure (exact task requirement)
        event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat() + "Z",  # ISO format with Z suffix
            "user_id": random.choice(self.users),
            "event_type": event_type,
            "product": {
                "id": product["id"],
                "name": product["name"], 
                "description": product["description"],
                "category": product["category"],
                "price": product["price"]
            },
            "session_id": random.choice(self.sessions)
        }
        
        # Add event-specific optional fields
        if event_type == "purchase":
            quantity = random.randint(1, 3)
            event.update({
                "quantity": quantity,
                "total_amount": product["price"] * quantity,
                "payment_method": random.choice(self.payment_methods)
            })
        elif event_type == "add_to_cart":
            event["quantity"] = random.randint(1, 5)
        elif event_type == "view":
            event["view_duration"] = random.randint(5, 300)  # seconds
        elif event_type == "search":
            # For search events, we might not have a specific product
            search_terms = [
                "laptop", "telefon", "ayakkabı", "gömlek", "kulaklık",
                "masa", "kitap", "elbise", "spor", "elektronik"
            ]
            event["search_query"] = random.choice(search_terms)
        
        return event
    
    def generate_batch(self, count: int) -> List[Dict[str, Any]]:
        """Generate specified number of events"""
        return [self.generate_event() for _ in range(count)]
    
    def save_to_file(self, events: List[Dict[str, Any]], filename: str):
        """Save events to JSONL file (one JSON per line)"""
        with open(filename, 'w', encoding='utf-8') as f:
            for event in events:
                f.write(json.dumps(event, ensure_ascii=False) + '\n')
        print(f"✅ {len(events)} events saved to {filename}")
    
    def send_to_kafka(self, events: List[Dict[str, Any]], 
                     topic: str = None, bootstrap_servers: str = None):
        """Send events to Kafka topic (Task requirement)"""
        topic = topic or os.getenv('KAFKA_TOPIC', 'ecommerce-events')
        bootstrap_servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                batch_size=int(os.getenv('KAFKA_BATCH_SIZE', '16384')),
                linger_ms=10,
                compression_type='gzip'
            )
            
            sent_count = 0
            for event in events:
                # Use user_id as partition key for consistent partitioning
                key = event.get('user_id', 'unknown')
                producer.send(topic, key=key, value=event)
                sent_count += 1
                
                # Progress indicator every 100 messages
                if sent_count % 100 == 0:
                    print(f"📤 {sent_count}/{len(events)} events sent...")
            
            producer.flush()
            producer.close()
            print(f"✅ {sent_count} events sent to Kafka topic '{topic}'")
            
        except Exception as e:
            print(f"❌ Kafka send error: {e}")
            print("💡 Make sure Kafka is running: docker compose up -d")
    
    def stream_to_kafka(self, events_per_second: int = None, duration_seconds: int = 60,
                       topic: str = None, bootstrap_servers: str = None):
        """
        Stream events to Kafka at specified rate (Task performance requirement)
        Task requirement: At least 1000 events per second capability
        """
        # Use environment variables (DevOps requirement)
        topic = topic or os.getenv('KAFKA_TOPIC', 'ecommerce-events')
        bootstrap_servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        events_per_second = events_per_second or int(os.getenv('PERFORMANCE_TARGET_EVENTS_PER_SEC', '100'))
        
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                batch_size=int(os.getenv('KAFKA_BATCH_SIZE', '16384')),
                linger_ms=5,
                compression_type='gzip',
                buffer_memory=33554432
            )
            
            interval = 1.0 / events_per_second
            total_events = 0
            start_time = time.time()
            
            print(f"🚀 Starting stream: {events_per_second} events/sec for {duration_seconds}s")
            print(f"📊 Target: {events_per_second * duration_seconds} total events")
            
            while time.time() - start_time < duration_seconds:
                event = self.generate_event()
                key = event.get('user_id', 'unknown')
                producer.send(topic, key=key, value=event)
                total_events += 1
                
                # Progress every 1000 events
                if total_events % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = total_events / elapsed
                    print(f"📈 {total_events} events sent | {rate:.1f} events/sec | {elapsed:.1f}s elapsed")
                
                time.sleep(interval)
            
            producer.flush()
            producer.close()
            
            elapsed = time.time() - start_time
            actual_rate = total_events / elapsed
            print(f"✅ Stream completed:")
            print(f"   📊 Total events: {total_events}")
            print(f"   ⏱️  Duration: {elapsed:.2f}s")
            print(f"   🚀 Actual rate: {actual_rate:.1f} events/sec")
            
        except Exception as e:
            print(f"❌ Kafka streaming error: {e}")
    
    def validate_event_structure(self, event: Dict[str, Any]) -> bool:
        """
        Validate event structure matches task requirements exactly
        """
        required_fields = ["event_id", "timestamp", "user_id", "event_type", "product", "session_id"]
        
        # Check top-level fields
        for field in required_fields:
            if field not in event:
                print(f"❌ Missing required field: {field}")
                return False
        
        # Check nested product structure (Task requirement)
        product = event.get("product", {})
        required_product_fields = ["id", "name", "description", "category", "price"]
        
        for field in required_product_fields:
            if field not in product:
                print(f"❌ Missing product field: {field}")
                return False
        
        # Validate data types
        if not isinstance(event.get("product", {}).get("price", 0), (int, float)):
            print(f"❌ Price must be numeric")
            return False
            
        return True


def main():
    """
    Main function for testing and demonstration
    VectorStream E-Commerce Behavior Analysis Pipeline
    """
    generator = ECommerceDataGenerator()
    
    print("=" * 60)
    print("� VectorStream: E-Commerce Behavior Analysis Pipeline")
    print("=" * 60)
    print("📋 Task Requirements:")
    print("  ✅ Apache Spark Structured Streaming")
    print("  ✅ Kafka event streaming") 
    print("  ✅ Nested product structure")
    print("  ✅ Sentence Transformers embedding")
    print("  ✅ Qdrant vector database")
    print("  ✅ Performance: 1000+ events/sec, <30s latency")
    print("=" * 60)
    
    # Generate test batch
    print("\n🧪 Generating test events...")
    events = generator.generate_batch(10)
    
    # Validate events
    print("\n� Validating event structure...")
    all_valid = True
    for i, event in enumerate(events, 1):
        if generator.validate_event_structure(event):
            print(f"  ✅ Event {i}: Valid structure")
        else:
            print(f"  ❌ Event {i}: Invalid structure")
            all_valid = False
    
    if all_valid:
        print("\n✅ All events have valid structure!")
        
        # Save sample to file
        generator.save_to_file(events, "sample_ecommerce_events.jsonl")
        
        # Show sample event
        print("\n📄 Sample event structure:")
        print(json.dumps(events[0], indent=2, ensure_ascii=False))
        
        # Performance test option
        print("\n🚀 Performance test options:")
        print("  1. Stream 1000 events/sec for 60 seconds (Task requirement test)")
        print("  2. Generate large batch and send to Kafka")
        print("  3. Exit")
        
        choice = input("\nSelect option (1-3): ").strip()
        
        if choice == "1":
            print("\n🎯 Starting performance test...")
            generator.stream_to_kafka(events_per_second=1000, duration_seconds=60)
        elif choice == "2":
            count = int(input("Enter number of events to generate: "))
            batch = generator.generate_batch(count)
            generator.send_to_kafka(batch)
        else:
            print("\n👋 Exiting...")
    else:
        print("\n❌ Some events have invalid structure!")


if __name__ == "__main__":
    main()