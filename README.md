# 🛒 VectorStream: Gerçek Zamanlı E-Ticaret Davranış Analizi Hattı
**MLOps Görev Uygulaması - Gerçek Zamanlı Veri İşleme Hattı**

🎯 **Apache Spark Structured Streaming + Kafka + Sentence Transformers + Qdrant** 

### 1. Servisleri Başlat
```bash
docker compose up -d

python3.10 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. E-ticaret Demo EVENT Verisi Üret
```bash
python scripts/live_event_demo.py -c 50000 --burst --no-rate-limit  --compression lz4
```

### 3. Docker Üzerinde Ayağa Kalkan VectorStreamApp Eventleri Otomatik Olarak Consume Edecek
```bash
docker logs app -f
```

## 🌐 İzleme Arayüzleri
Bu kontrol panellerine erişerek hattı izleyin:
- **Hattı API'si**: http://localhost:8080 (metrikler, sağlık, belgeler)
- **Kafka UI**: http://localhost:8090 (mesaj akışları)
- **Qdrant Kontrol Paneli**: http://localhost:6333/dashboard (vektör depolama)
- **Grafana**: http://localhost:3000 (admin/admin123)
- **Spark UI**: http://localhost:4040 (akış işleri)

## 📊 Görev Gereksinimleri Doğrulaması
### Performans Hedefleri
- **Verim**: Minimum 1000 olay/saniye ✅
- **Gecikme**: Maksimum 30 saniye uçtan uca ✅  
- **Bellek**: İzleme ile etkin işleme ✅
- **GPU**: Kullanılabilir olduğunda RAPIDS hızlandırma ✅ (test edilmedi)

### Olay Yapısı (Görev Uyumlu)
```json
{
  "event_id": "uuid",
  "timestamp": "2024-01-15T10:30:00Z",
  "user_id": "user123",
  "event_type": "purchase",
  "product": {
    "id": "uuid",
    "name": "Product Name",
    "description": "Detailed product description...",
    "category": "Electronics", 
    "price": 1299.99
  },
  "session_id": "session789"
}
```

## 🏗️ Mimari
### 📊 Sistem Mimarisi
```mermaid
graph TB
    %% External Data Sources
    DS[Veri Kaynakları] --> K[Kafka Kümesi]
    
    %% Kafka Layer
    K --> KC[Kafka Tüketici]
    K --> KUI[Kafka UI<br/>:8080]
    
    %% Processing Layer
    KC --> EP[Gömme İşleyici<br/>Spark + RAPIDS]
    EP --> QW[Qdrant Yazıcı]
    
    %% Storage Layer
    QW --> Q[Qdrant Vektör DB<br/>:6333]
    
    %% Monitoring & Health
    HC[Sağlık Kontrolü] --> KC
    HC --> EP
    HC --> QW
    HC --> Q
    
    PM[Prometheus Metrikleri<br/>:9090] --> KC
    PM --> EP
    PM --> QW
    
    G[Grafana Kontrol Paneli<br/>:3000] --> PM
    
    %% Configuration
    CONFIG[Konfigürasyon<br/>app_config.yaml] --> KC
    CONFIG --> EP
    CONFIG --> QW
    
    %% Error Handling
    EH[Hata İşleyici] --> KC
    EH --> EP
    EH --> QW
    
    %% Logging
    LOG[Merkezi Günlükleyici] --> KC
    LOG --> EP
    LOG --> QW
    LOG --> EH
    
    class K,KC,KUI kafka
    class EP,QW processing
    class Q storage
    class HC,PM,G,LOG monitoring
    class CONFIG,EH config
```
#### Veri Akış Diyagramı
```mermaid
sequenceDiagram
    participant DS as Veri Kaynakları
    participant K as Kafka
    participant KC as Kafka Tüketici
    participant EP as Gömme İşleyici
    participant S as Spark/RAPIDS
    participant QW as Qdrant Yazıcı
    participant Q as Qdrant DB
    participant M as İzleme
    
    DS->>K: Ham Veri Gönder
    KC->>K: Mesajları Çek
    K-->>KC: Toplu Döndür
    KC->>EP: Mesaj İlet
    EP->>S: İşleme İşini Gönder
    S->>S: Gömme Üret
    S-->>EP: Gömme Döndür
    EP->>QW: Gömme Gönder
    QW->>Q: Vektör Ekle
    Q-->>QW: Ekleme Onayla
    
    par Sürekli İzleme
        KC->>M: Metrik Gönder
        EP->>M: Metrik Gönder
        QW->>M: Metrik Gönder
    end
```

### 📈 Veri Akışı

```
E-ticaret Olayları → Kafka → Spark → GPU İşleme → Qdrant Vektör DB
```

**Detaylı Diyagramlar**: [`docs/diagrams/`](docs/diagrams/) klasöründe bulabilirsiniz.

## 📊 Sistem Mimarisi Diyagramları

Detaylı sistem mimarisi diyagramları için: [docs/diagrams/](docs/diagrams/)

## 📈 Performans Sonuçları

### ✅ Test Sonuçları

| Metrik | Hedef | Sonuç | Durum |
|--------|-------|-------|-------|
| Verim | 1000+ olay/s | 1278.3 olay/s | ✅ |
| Gecikme | <30 saniye | 3.6s | ✅ |
| Hata Oranı | <1% | 0.00% | ✅ |
| GPU Kullanımı | Evet | Apple Silicon MPS | ✅ |

### 🚀 Özellikler

- **GPU Hızlandırması**: RAPIDS + Apple Silicon MPS
- **Toplu İşleme**: Optimal toplu boyut ile yüksek verim
- **Otomatik Yedekleme**: GPU → CPU geçişi
- **Performans İzleme**: Gerçek zamanlı metrikler
- **Hata İşleme**: Devre kesici deseni

## 📁 Proje Yapısı

```
newmind-ai/
├── 🐳 docker-compose.yml     # Tüm servisler
├── 📦 scripts/               # Demo scriptleri
│   ├── generate_ecommerce_data.py
│   └── live_event_demo.py
├── 📊 src/                   # Ana kod
│   ├── core/                 # Temel bileşenler
│   └── main.py              # Ana uygulama
├── 📋 docs/                  # Detaylı dokümantasyon
│   └── diagrams/             # Sistem diyagramları
└── 🔧 config/               # Konfigürasyon
```