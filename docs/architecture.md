# Sistem Mimarisi

## Genel Bakış

E-ticaret davranış analizi için gerçek zamanlı veri işleme sistemi.

### Teknoloji Stack'i

- **Apache Kafka**: Gerçek zamanlı veri akışı
- **Apache Spark**: Dağıtık veri işleme
- **RAPIDS GPU**: Hızlandırılmış embedding işleme
- **Qdrant**: Vektör veritabanı
- **Docker**: Konteynerizasyon

### Performans Hedefleri

- **Throughput**: 1000+ event/saniye
- **Latency**: <30 saniye
- **Error Rate**: <%1
- **GPU Kullanımı**: RAPIDS ile hızlandırma

## Ana Bileşenler

### 1. Kafka (Port: 9092)
- E-ticaret event'lerini alır
- Mesaj kuyruğu görevi görür
- Kafka UI: `localhost:8090`

### 2. Spark Cluster
- Master: `localhost:8080`
- Worker: Dağıtık işleme
- RAPIDS GPU hızlandırması

### 3. Qdrant (Port: 6333)
- Vektör veritabanı
- Similarity search
- Dashboard: `localhost:6333/dashboard`

### 4. Monitoring
- **Prometheus**: `localhost:9090`
- **Grafana**: `localhost:3000` (admin/admin123)

## Özellikler

- **GPU Hızlandırması**: RAPIDS ile 10x performans
- **Batch İşleme**: Optimize edilmiş throughput
- **Otomatik Fallback**: GPU → CPU geçişi
- **Performans İzleme**: Gerçek zamanlı metrikler
- **Hata Yönetimi**: Circuit breaker pattern

## Veri Akışı

```
E-ticaret Events → Kafka → Spark/RAPIDS → Qdrant
```

### Adım Adım İşlem

1. **Event Girişi**: E-ticaret event'leri Kafka'ya gönderilir
2. **Mesaj Tüketimi**: Spark Kafka'dan mesajları okur
3. **GPU İşleme**: RAPIDS ile embedding üretimi
4. **Vektör Depolama**: Qdrant'a kaydetme
5. **Monitoring**: Performans metriklerini izleme

## Demo için Hızlı Başlangıç

```bash
# Tüm servisleri başlat
docker-compose up -d

# Demo verisi üret
python scripts/generate_ecommerce_data.py

# Performans testi
python scripts/ecommerce_performance_test.py
```

## Detaylı Diyagramlar

Sistem diyagramları için: [`docs/diagrams/`](diagrams/) klasörüne bakın.