# Hata Yönetimi

## Genel Bakış

VectorStream sistemi, demo sırasında karşılaşılabilecek yaygın hataları otomatik olarak yönetir.

## Yaygın Hatalar ve Çözümleri

### Kafka Bağlantı Hataları
```bash
# Kafka servisini yeniden başlat
docker-compose restart kafka

# Kafka loglarını kontrol et
docker-compose logs kafka
```

### Qdrant Bağlantı Hataları
```bash
# Qdrant servisini yeniden başlat
docker-compose restart qdrant

# Qdrant durumunu kontrol et
curl http://localhost:6333/health
```

### GPU/Memory Hataları
```bash
# Sistem kaynaklarını kontrol et
docker stats

# Servisleri yeniden başlat
docker-compose down
docker-compose up -d
## Otomatik Kurtarma Özellikleri

### Circuit Breaker
Sistem, başarısız bağlantıları otomatik olarak tespit eder ve geçici olarak devre dışı bırakır.

### Retry Mekanizması
Başarısız işlemler otomatik olarak yeniden denenir.

### GPU Fallback
GPU hataları durumunda sistem otomatik olarak CPU'ya geçer.

### Health Monitoring
Tüm bileşenler sürekli izlenir ve sorunlar otomatik olarak raporlanır.

## Demo İçin Hızlı Çözümler

### Sistem Yeniden Başlatma
```bash
# Tüm servisleri yeniden başlat
docker-compose restart

# Belirli bir servisi yeniden başlat
docker-compose restart [service-name]
```

### Log Kontrolü
```bash
# Tüm servislerin loglarını görüntüle
docker-compose logs -f

# Belirli bir servisin loglarını görüntüle
docker-compose logs -f [service-name]
```

### Sistem Durumu
```bash
# Servis durumlarını kontrol et
docker-compose ps

# Sistem kaynaklarını kontrol et
docker stats
```