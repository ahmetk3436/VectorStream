# API Referansı

## Genel Bakış

VectorStream sistemi, sağlık kontrolü, metrik toplama ve DLQ yönetimi için RESTful API endpoint'leri sağlar. Bu API'ler demo sırasında sistem durumunu izlemek ve yönetmek için kullanılır.

## Base URL

```
Health Check Server: http://localhost:8080
Metrics Server: http://localhost:9090
DLQ Management: http://localhost:8080/dlq
```

## Sağlık Kontrolü Endpoint'leri

### 1. Genel Sağlık Durumu

**Endpoint:** `GET /health`

**Açıklama:** Sistemin genel sağlık durumunu döndürür. Tüm servislerin (Kafka, Qdrant, sistem kaynakları) durumunu kontrol eder.

**Response:**
```json
{
  "status": "healthy|degraded|unhealthy",
  "checks": [
    {
      "service": "kafka",
      "status": "healthy",
      "message": "Kafka connection successful",
      "timestamp": 1640995200.123,
      "details": {
        "bootstrap_servers": "localhost:9092",
        "topics": ["ecommerce-events"],
        "consumer_lag": 0
      }
    },
    {
      "service": "qdrant",
      "status": "healthy",
      "message": "Qdrant connection successful",
      "timestamp": 1640995200.456,
      "details": {
        "host": "localhost:6333",
        "collection": "ecommerce_embeddings",
        "vector_count": 1500,
        "index_status": "green"
      }
    },
    {
      "service": "system",
      "status": "healthy",
      "message": "System resources within normal limits",
      "timestamp": 1640995200.789,
      "details": {
        "cpu_usage": 45.2,
        "memory_usage": 67.8,
        "disk_usage": 23.1
      }
    }
  ],
  "uptime": 3661.45,
  "version": "1.0.0"
}
```

**HTTP Status Kodları:**
- `200`: Sistem sağlıklı veya kısmen sağlıklı
- `503`: Sistem sağlıksız

### 2. Servis Bazlı Sağlık Kontrolü

**Endpoint:** `GET /health/{service}`

**Parametreler:**
- `service`: kafka, qdrant, system

**Açıklama:** Belirli bir servisin detaylı sağlık durumunu döndürür.

**Response:**
```json
{
  "service": "kafka",
  "status": "healthy",
  "message": "All checks passed",
  "timestamp": 1640995200.123,
  "checks": [
    {
      "name": "connection",
      "status": "healthy",
      "message": "Connected to bootstrap servers"
    },
    {
      "name": "topics",
      "status": "healthy",
      "message": "All required topics available"
    },
    {
      "name": "consumer_lag",
      "status": "healthy",
      "message": "Consumer lag within acceptable limits"
    }
  ]
}
```

### 3. Sistem Metrikleri

**Endpoint:** `GET /metrics`

**Açıklama:** Prometheus formatında sistem metriklerini döndürür.

**Response:** (Prometheus format)
```
# HELP kafka_messages_processed_total Total number of Kafka messages processed
# TYPE kafka_messages_processed_total counter
kafka_messages_processed_total{topic="ecommerce-events",status="success"} 1542

# HELP qdrant_embeddings_written_total Total number of embeddings written to Qdrant
# TYPE qdrant_embeddings_written_total counter
qdrant_embeddings_written_total{collection="ecommerce_embeddings"} 1489

# HELP system_cpu_usage Current CPU usage percentage
# TYPE system_cpu_usage gauge
system_cpu_usage 45.2

# HELP processing_duration_seconds Time spent processing messages
# TYPE processing_duration_seconds histogram
processing_duration_seconds_bucket{le="0.1"} 890
processing_duration_seconds_bucket{le="0.5"} 1200
processing_duration_seconds_bucket{le="1.0"} 1450
processing_duration_seconds_bucket{le="+Inf"} 1542
```

## Dead Letter Queue (DLQ) API'leri

### 1. DLQ İstatistikleri

**Endpoint:** `GET /dlq/stats`

**Açıklama:** DLQ sistem istatistiklerini döndürür.

**Response:**
```json
{
  "total_failed": 15,
  "in_retry_queue": 3,
  "ready_for_retry": 1,
  "failure_reasons": {
    "processing_error": 8,
    "timeout_error": 4,
    "validation_error": 3
  },
  "dlq_path": "/data/dlq",
  "config": {
    "max_retries": 3,
    "retry_delays": [60, 300, 900],
    "enable_retry": true,
    "batch_size": 100
  }
}
```

### 2. DLQ Mesajlarını Listele

**Endpoint:** `GET /dlq/messages`

**Query Parametreleri:**
- `limit` (int): Maksimum mesaj sayısı (varsayılan: 10)
- `offset` (int): Başlangıç offset (varsayılan: 0)
- `status` (string): failed, retry
- `reason` (string): Hata nedeni filtresi

**Response:**
```json
{
  "messages": [
    {
      "id": "dlq_001",
      "original_topic": "ecommerce-events",
      "failure_reason": "processing_error",
      "error_details": "JSON parsing failed",
      "attempt_count": 2,
      "first_failure_time": "2024-01-15T10:00:00Z",
      "last_failure_time": "2024-01-15T10:05:00Z",
      "retry_after": "2024-01-15T10:10:00Z",
      "original_message": {
        "user_id": "user123",
        "action": "purchase",
        "product_id": "prod456"
      }
    }
  ],
  "total_count": 15,
  "has_more": true
}
```

### 3. DLQ Mesajı Replay

**Endpoint:** `POST /dlq/replay/{message_id}`

**Request Body:**
```json
{
  "target_topic": "ecommerce-events-replay"  // Opsiyonel
}
```

**Response:**
```json
{
  "success": true,
  "message": "Message replayed successfully",
  "replayed_to_topic": "ecommerce-events",
  "replay_timestamp": "2024-01-15T10:15:00Z"
}
```

### 4. Toplu DLQ Replay

**Endpoint:** `POST /dlq/replay/batch`

**Request Body:**
```json
{
  "message_ids": ["dlq_001", "dlq_002", "dlq_003"],
  "target_topic": "ecommerce-events-replay",
  "filter": {
    "reason": "processing_error",
    "max_retry_count": 2
  }
}
```

**Response:**
```json
{
  "total_attempted": 3,
  "successful": 2,
  "failed": 1,
  "results": [
    {
      "message_id": "dlq_001",
      "success": true,
      "replay_timestamp": "2024-01-15T10:15:00Z"
    },
    {
      "message_id": "dlq_002",
      "success": true,
      "replay_timestamp": "2024-01-15T10:15:01Z"
    },
    {
      "message_id": "dlq_003",
      "success": false,
      "error": "Message not found"
    }
  ]
}
```

### 5. Manuel DLQ Mesajı Ekleme

**Endpoint:** `POST /dlq/messages`

**Request Body:**
```json
{
  "original_topic": "ecommerce-events",
  "original_partition": 0,
  "original_offset": 12345,
  "original_key": "user123",
  "original_value": "{\"user_id\":\"user123\",\"action\":\"purchase\"}",
  "failure_reason": "processing_error",
  "error_message": "Manual DLQ entry for testing"
}
```

**Response:**
```json
{
  "success": true,
  "message_id": "dlq_004",
  "created_at": "2024-01-15T10:20:00Z"
}
```

## Konfigürasyon API'leri

### 1. Sistem Konfigürasyonu

**Endpoint:** `GET /config`

**Response:**
```json
{
  "kafka": {
    "bootstrap_servers": "localhost:9092",
    "topic": "ecommerce-events",
    "group_id": "vectorstream-consumer",
    "auto_offset_reset": "latest"
  },
  "qdrant": {
    "host": "localhost",
    "port": 6333,
    "collection_name": "ecommerce_embeddings",
    "vector_size": 384
  },
  "dlq": {
    "max_retries": 3,
    "retry_delays": [60, 300, 900],
    "enable_retry": true
  },
  "monitoring": {
    "health_check_interval": 30,
    "metrics_collection_interval": 10
  }
}
```

### 2. Konfigürasyon Güncelleme

**Endpoint:** `PUT /config/{section}`

**Request Body:**
```json
{
  "max_retries": 5,
  "retry_delays": [30, 120, 300, 600, 1800],
  "enable_retry": true,
  "batch_size": 50
}
```

**Response:**
```json
{
  "success": true,
  "message": "DLQ configuration updated successfully",
  "updated_at": "2024-01-15T10:25:00Z"
}
```

## İzleme ve Operasyonel API'ler

### 1. Sistem Logları

**Endpoint:** `GET /logs`

**Query Parametreleri:**
- `level` (string): debug, info, warning, error
- `since` (string): ISO timestamp
- `limit` (int): Maksimum log sayısı

**Response:**
```json
{
  "logs": [
    {
      "timestamp": "2024-01-15T10:30:00Z",
      "level": "error",
      "logger": "dlq_manager",
      "message": "Failed to process message: JSON parsing error",
      "context": {
        "message_id": "msg_001",
        "topic": "ecommerce-events"
      }
    }
  ]
}
```

### 2. Performans İstatistikleri

**Endpoint:** `GET /stats/performance`

**Response:**
```json
{
  "throughput": {
    "messages_per_second": 125.5,
    "embeddings_per_second": 118.2
  },
  "latency": {
    "avg_processing_time_ms": 45.2,
    "p95_processing_time_ms": 120.5,
    "p99_processing_time_ms": 250.1
  },
  "error_rates": {
    "total_errors": 15,
    "error_rate_percent": 0.97,
    "retry_success_rate": 75.0
  }
}
```

### 3. Kaynak Kullanımı

**Endpoint:** `GET /stats/resources`

**Response:**
```json
{
  "cpu": {
    "usage_percent": 45.2,
    "cores": 8
  },
  "memory": {
    "used_mb": 1024,
    "total_mb": 4096,
    "usage_percent": 25.0
  },
  "disk": {
    "used_gb": 15.5,
    "total_gb": 100,
    "usage_percent": 15.5
  },
  "network": {
    "bytes_in": 1048576,
    "bytes_out": 524288
  }
}
```

## Hata Kodları ve Yanıtları

### HTTP Status Kodları

- `200 OK`: İstek başarılı
- `201 Created`: Kaynak başarıyla oluşturuldu
- `400 Bad Request`: Geçersiz istek parametreleri
- `401 Unauthorized`: Kimlik doğrulama gerekli
- `403 Forbidden`: Yetkisiz erişim
- `404 Not Found`: Kaynak bulunamadı
- `429 Too Many Requests`: Oran sınırı aşıldı
- `500 Internal Server Error`: Sunucu hatası
- `503 Service Unavailable`: Servis kullanılamıyor

### Hata Yanıt Formatı

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid message format",
    "details": {
      "field": "original_value",
      "reason": "JSON parsing failed"
    },
    "timestamp": "2024-01-15T10:35:00Z",
    "request_id": "req_12345"
  }
}
```

## Rate Limiting

API'ler rate limiting ile korunmaktadır:

- **Genel API'ler**: 100 istek/dakika
- **DLQ İşlemleri**: 50 istek/dakika
- **Toplu İşlemler**: 10 istek/dakika

Rate limit aşıldığında `429 Too Many Requests` yanıtı döner:

```json
{
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "Too many requests",
    "retry_after": 60
  }
}
```

## Kimlik Doğrulama

Production ortamında API'ler API key ile korunmaktadır:

```bash
curl -H "X-API-Key: your-api-key" http://localhost:8080/health
```

## SDK ve Client Kütüphaneleri

### Python Client

```python
from vectorstream_client import VectorStreamClient

client = VectorStreamClient(
    base_url="http://localhost:8080",
    api_key="your-api-key"
)

# Sağlık kontrolü
health = client.health.get_status()

# DLQ işlemleri
dlq_stats = client.dlq.get_stats()
messages = client.dlq.list_messages(limit=50)
client.dlq.replay_message("dlq_001")
```

### JavaScript Client

```javascript
import { VectorStreamClient } from 'vectorstream-js-client';

const client = new VectorStreamClient({
  baseUrl: 'http://localhost:8080',
  apiKey: 'your-api-key'
});

// Async/await kullanımı
const health = await client.health.getStatus();
const dlqStats = await client.dlq.getStats();
```

## WebSocket API'leri

Gerçek zamanlı güncellemeler için WebSocket desteği:

```javascript
const ws = new WebSocket('ws://localhost:8080/ws/events');

ws.onmessage = function(event) {
  const data = JSON.parse(event.data);
  console.log('Real-time event:', data);
};
```

WebSocket mesaj formatları:

```json
{
  "type": "health_update",
  "data": {
    "service": "kafka",
    "status": "healthy",
    "timestamp": "2024-01-15T10:40:00Z"
  }
}

{
  "type": "dlq_message",
  "data": {
    "action": "new_failed_message",
    "message_id": "dlq_005",
    "failure_reason": "timeout_error"
  }
}

{
  "type": "metrics_update",
  "data": {
    "messages_processed": 1543,
    "error_count": 16,
    "timestamp": "2024-01-15T10:40:00Z"
  }
}
```
- `503`: Sistem sağlıksız
- `500`: Sağlık kontrolü hatası

### 2. Liveness Probe

**Endpoint:** `GET /health/live`

**Açıklama:** Kubernetes liveness probe için kullanılır. Uygulamanın çalışır durumda olup olmadığını kontrol eder.

**Response:**
```json
{
  "status": "alive|dead",
  "timestamp": 1640995200.123
}
```

**HTTP Status Kodları:**
- `200`: Uygulama çalışıyor
- `503`: Uygulama çalışmıyor (restart gerekli)

### 3. Readiness Probe

**Endpoint:** `GET /health/ready`

**Açıklama:** Kubernetes readiness probe için kullanılır. Uygulamanın trafik almaya hazır olup olmadığını kontrol eder.

**Response:**
```json
{
  "status": "ready|not ready",
  "timestamp": 1640995200.123
}
```

**HTTP Status Kodları:**
- `200`: Uygulama hazır
- `503`: Uygulama hazır değil

### 4. Kafka Sağlık Kontrolü

**Endpoint:** `GET /health/kafka`

**Açıklama:** Kafka bağlantısının durumunu kontrol eder.

**Response:**
```json
{
  "service": "kafka",
  "status": "healthy|degraded|unhealthy",
  "message": "Kafka connection successful",
  "timestamp": 1640995200.123
}
```

**HTTP Status Kodları:**
- `200`: Kafka bağlantısı sağlıklı
- `503`: Kafka bağlantısı sorunlu

### 5. Qdrant Sağlık Kontrolü

**Endpoint:** `GET /health/qdrant`

**Açıklama:** Qdrant veritabanı bağlantısının durumunu kontrol eder.

**Response:**
```json
{
  "service": "qdrant",
  "status": "healthy|degraded|unhealthy",
  "message": "Qdrant connection successful",
  "timestamp": 1640995200.123
}
```

**HTTP Status Kodları:**
- `200`: Qdrant bağlantısı sağlıklı
- `503`: Qdrant bağlantısı sorunlu

### 6. Sistem Sağlık Kontrolü

**Endpoint:** `GET /health/system`

**Açıklama:** Sistem kaynaklarının (CPU, bellek, disk) durumunu kontrol eder.

**Response:**
```json
{
  "service": "system",
  "status": "healthy|degraded|unhealthy",
  "message": "CPU: 45%, Memory: 60%, Disk: 30%",
  "timestamp": 1640995200.123
}
```

**HTTP Status Kodları:**
- `200`: Sistem kaynakları normal
- `503`: Sistem kaynakları kritik seviyede

## Metrik Endpoint'leri

### 1. Prometheus Metrikleri

**Endpoint:** `GET /metrics`

**Açıklama:** Prometheus formatında sistem metriklerini döndürür.

**Response Format:** Prometheus text format

**Örnek Metrikler:**
```
# HELP kafka_messages_processed_total Total number of processed Kafka messages
# TYPE kafka_messages_processed_total counter
kafka_messages_processed_total{topic="embeddings"} 1234

# HELP qdrant_operations_duration_seconds Duration of Qdrant operations
# TYPE qdrant_operations_duration_seconds histogram
qdrant_operations_duration_seconds_bucket{operation="search",le="0.1"} 100
qdrant_operations_duration_seconds_bucket{operation="search",le="0.5"} 150
qdrant_operations_duration_seconds_bucket{operation="search",le="1.0"} 180

# HELP system_cpu_usage_percent Current CPU usage percentage
# TYPE system_cpu_usage_percent gauge
system_cpu_usage_percent 45.2

# HELP system_memory_usage_percent Current memory usage percentage
# TYPE system_memory_usage_percent gauge
system_memory_usage_percent 67.8
```

## Demo İçin Önemli Metrikler

### Temel Metrikler
- `kafka_messages_processed_total`: İşlenen mesaj sayısı
- `qdrant_operations_total`: Qdrant operasyon sayısı
- `system_cpu_usage_percent`: CPU kullanımı
- `system_memory_usage_percent`: Bellek kullanımı

### Sağlık Durumu
- `healthy`: Servis sağlıklı
- `unhealthy`: Servis sağlıksız

## Demo İçin Hızlı Kullanım

### Temel Komutlar
```bash
# Sistem sağlığını kontrol et
curl http://localhost:8080/health

# Prometheus metriklerini görüntüle
curl http://localhost:9090/metrics
```

### Web Arayüzleri
- **Grafana**: http://localhost:3000 (admin/admin123)
- **Prometheus**: http://localhost:9090
- **Kafka UI**: http://localhost:8090
- **Qdrant Dashboard**: http://localhost:6333/dashboard