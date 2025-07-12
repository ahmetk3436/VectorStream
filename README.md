# NewMind-AI: Real-time Embedding Processing Pipeline

🚀 **Kafka + Spark + Qdrant** tabanlı gerçek zamanlı vektör işleme sistemi

## 📊 Proje Durumu

✅ **Tamamlanan Bileşenler:**
- Docker Compose altyapısı (Kafka, Qdrant, Spark)
- Temel konfigürasyon sistemi
- Logger altyapısı
- Kafka Consumer
- Qdrant Writer
- Ana uygulama entegrasyonu

🔄 **Geliştirme Aşamasında:**
- Spark embedding processor
- Monitoring ve metrics
- Error handling
- Performance optimization

## 🎯 Progressive Development Roadmap

### Aşama 1: Temel Altyapı ✅
```bash
# Import hatalarını çöz
python test_imports.py

# Adım adım test
python test_step_by_step.py
```

### Aşama 2: End-to-End Test 🔄
```bash
# 1. Servisleri başlat
docker-compose up -d

# 2. Bağımlılıkları yükle
pip install -r requirements.txt

# 3. Ana uygulamayı çalıştır
python src/main.py

# 4. Başka terminalde test mesajı gönder
python test_step_by_step.py --send-message
```

### Aşama 3: Monitoring Ekleme 📊
- Prometheus metrics
- Health check endpoints
- Grafana dashboards

### Aşama 4: Spark Integration ⚡
- Gerçek ML modeli entegrasyonu
- Batch processing
- GPU acceleration (RAPIDS)

### Aşama 5: Production Ready 🏭
- Comprehensive error handling
- Performance optimization
- Kubernetes deployment

## 🧪 Test Komutları

```bash
# Temel import testleri
python test_imports.py

# Progressive development testleri
python test_step_by_step.py

# Kafka mesaj gönderme testi
python test_step_by_step.py --send-message

# Ana uygulamayı çalıştır
python src/main.py
```

## 📁 Proje Yapısı

```
newmind-ai/
├── 📋 config/                 # Konfigürasyon dosyaları
│   ├── app_config.yaml       # Ana konfigürasyon
│   └── kafka_config.py       # Kafka ayarları
├── 🐳 docker-compose.yml     # Servis tanımları
├── 📦 src/                   # Ana kaynak kod
│   ├── core/                 # Temel bileşenler
│   │   ├── kafka_consumer.py # Kafka mesaj tüketimi
│   │   ├── qdrant_writer.py  # Vektör veritabanı
│   │   └── embedding_processor.py # ML işleme
│   ├── utils/                # Yardımcı araçlar
│   │   └── logger.py         # Loglama sistemi
│   └── main.py              # Ana uygulama
├── 🧪 test_imports.py        # Import testleri
├── 🔄 test_step_by_step.py   # Progressive testler
└── 📊 logs/                  # Log dosyaları
```

## 🔧 Geliştirme Ortamı Kurulumu

1. **Docker Servislerini Başlat:**
   ```bash
   docker-compose up -d
   ```

2. **Python Bağımlılıklarını Yükle:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Testleri Çalıştır:**
   ```bash
   python test_imports.py
   python test_step_by_step.py
   ```

## 📊 Servis Portları

- **Kafka:** `localhost:9092`
- **Kafka UI:** `localhost:8090`
- **Qdrant:** `localhost:6333`
- **Spark Master:** `localhost:8080`
- **Spark History:** `localhost:18080`

## 🎯 Sonraki Adımlar

### Hemen Yapılabilecekler:
1. `python src/main.py` ile ana uygulamayı çalıştır
2. `python test_step_by_step.py --send-message` ile test mesajı gönder
3. Logs klasöründeki `app.log` dosyasını takip et
4. Qdrant UI'da (`localhost:6333/dashboard`) vektörleri görüntüle

### Geliştirme Önerileri:
1. **Monitoring:** Prometheus metrics ekle
2. **Error Handling:** Kapsamlı hata yönetimi
3. **Performance:** Batch processing optimize et
4. **ML Model:** Gerçek embedding modeli entegre et

## 🐛 Troubleshooting

### Import Hataları:
```bash
python test_imports.py  # Tüm import'ları test et
```

### Docker Sorunları:
```bash
docker-compose down
docker-compose up -d
docker-compose ps  # Servis durumlarını kontrol et
```

### Log Takibi:
```bash
tail -f logs/app.log  # Gerçek zamanlı log takibi
```

---

**🎉 Proje hazır! Artık progressive development ile adım adım geliştirebilirsiniz.**