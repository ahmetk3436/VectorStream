# VectorStream: E-Ticaret Davranış Analizi - API Referansı

## Genel Bakış

**NewMind AI Şirketi MLOps Task Çözümü**

Bu dokümantasyon, NewMind AI şirketi tarafından verilen "VectorStream: Gerçek Zamanlı E-Ticaret Davranış Analizi Pipeline'ı" task'ının API endpoint'lerini, health check mekanizmalarını ve monitoring araçlarını detaylandırır. Sistem, production-ready monitoring ve observability için FastAPI framework'ü kullanarak comprehensive health checks ve metrics collection sağlar.

### Task Monitoring Gereksinimleri
- **Health Checks**: Sistem bileşenlerinin durumu (Kafka, Qdrant, System)
- **Performance Metrics**: Throughput, latency, error rates
- **GPU Monitoring**: RAPIDS cuML resource utilization
- **Business Metrics**: E-ticaret event processing analytics

NewMind AI sistemi, sağlık kontrolü ve metrik toplama için RESTful API endpoint'leri sağlar. Bu API'ler sistem durumunu izlemek, performans metriklerini toplamak ve Kubernetes ortamında liveness/readiness probe'ları için kullanılır.

## Base URL

```
Health Check Server: http://localhost:8080
Metrics Server: http://localhost:9090
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
      "timestamp": 1640995200.123
    },
    {
      "service": "qdrant",
      "status": "healthy",
      "message": "Qdrant connection successful",
      "timestamp": 1640995200.456
    },
    {
      "service": "system",
      "status": "healthy",
      "message": "System resources within normal limits",
      "timestamp": 1640995200.789
    }
  ]
}
```

**HTTP Status Kodları:**
- `200`: Sistem sağlıklı veya kısmen sağlıklı
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

## Metrik Kategorileri

### Kafka Metrikleri
- `kafka_messages_processed_total`: İşlenen toplam mesaj sayısı
- `kafka_messages_failed_total`: Başarısız mesaj sayısı
- `kafka_consumer_lag`: Consumer lag değeri
- `kafka_connection_status`: Bağlantı durumu

### Qdrant Metrikleri
- `qdrant_operations_duration_seconds`: Operasyon süreleri
- `qdrant_operations_total`: Toplam operasyon sayısı
- `qdrant_search_results_total`: Arama sonuç sayısı
- `qdrant_connection_status`: Bağlantı durumu

### Embedding Metrikleri
- `embedding_processing_duration_seconds`: Embedding işleme süreleri
- `embedding_processing_total`: Toplam embedding sayısı
- `embedding_errors_total`: Embedding hata sayısı

### Sistem Metrikleri
- `system_cpu_usage_percent`: CPU kullanım yüzdesi
- `system_memory_usage_percent`: Bellek kullanım yüzdesi
- `system_disk_usage_percent`: Disk kullanım yüzdesi
- `system_uptime_seconds`: Sistem çalışma süresi

### Hata Metrikleri
- `errors_total`: Toplam hata sayısı (component ve error_type etiketli)
- `circuit_breaker_state`: Circuit breaker durumu
- `retry_attempts_total`: Toplam retry denemesi

## Hata Kodları ve Mesajları

### HTTP Status Kodları
- `200 OK`: İstek başarılı
- `503 Service Unavailable`: Servis kullanılamıyor
- `500 Internal Server Error`: Sunucu hatası

### Sağlık Durumu Değerleri
- `healthy`: Servis tamamen sağlıklı
- `degraded`: Servis kısmen sağlıklı (uyarı seviyesi)
- `unhealthy`: Servis sağlıksız (kritik seviye)
- `error`: Sağlık kontrolü hatası

## Kullanım Örnekleri

### cURL ile Sağlık Kontrolü
```bash
# Genel sağlık durumu
curl http://localhost:8080/health

# Liveness probe
curl http://localhost:8080/health/live

# Readiness probe
curl http://localhost:8080/health/ready

# Kafka sağlık kontrolü
curl http://localhost:8080/health/kafka

# Prometheus metrikleri
curl http://localhost:9090/metrics
```

### Python ile API Kullanımı
```python
import requests
import json

# Sağlık durumunu kontrol et
response = requests.get('http://localhost:8080/health')
health_data = response.json()

print(f"Sistem durumu: {health_data['status']}")
for check in health_data['checks']:
    print(f"{check['service']}: {check['status']} - {check['message']}")

# Metrikleri al
metrics_response = requests.get('http://localhost:9090/metrics')
print(metrics_response.text)
```

### Kubernetes Health Check Konfigürasyonu
```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: newmind-ai
    image: newmind-ai:latest
    livenessProbe:
      httpGet:
        path: /health/live
        port: 8080
      initialDelaySeconds: 30
      periodSeconds: 10
    readinessProbe:
      httpGet:
        path: /health/ready
        port: 8080
      initialDelaySeconds: 5
      periodSeconds: 5
```

## Monitoring Entegrasyonu

### Prometheus Konfigürasyonu
```yaml
scrape_configs:
  - job_name: 'newmind-ai'
    static_configs:
      - targets: ['localhost:9090']
    scrape_interval: 15s
    metrics_path: /metrics
```

### Grafana Dashboard
Metrikler Grafana dashboard'larında görselleştirilebilir:
- Sistem performans metrikleri
- Kafka mesaj işleme oranları
- Qdrant operasyon süreleri
- Hata oranları ve trend analizi

## Güvenlik

### Erişim Kontrolü
- Health check endpoint'leri genellikle public erişime açıktır
- Metrics endpoint'leri internal network'te tutulmalıdır
- Production ortamında authentication/authorization eklenebilir

### Rate Limiting
- API endpoint'lerine rate limiting uygulanabilir
- Özellikle metrics endpoint'leri için önemlidir

## Sınırlamalar

- Health check timeout'ları: 30 saniye
- Metrics toplama sıklığı: 15 saniye
- Maksimum eşzamanlı istek sayısı: 100
- Response boyutu limiti: 10MB

## Sorun Giderme

### Yaygın Sorunlar
1. **503 Service Unavailable**: Bağımlı servisler (Kafka/Qdrant) erişilemez
2. **Timeout**: Network bağlantı sorunları
3. **500 Internal Server Error**: Uygulama konfigürasyon hatası

### Debug Bilgileri
- Log seviyesini DEBUG'a ayarlayın
- Health check detaylarını inceleyin
- Metrics'teki hata sayılarını kontrol edin