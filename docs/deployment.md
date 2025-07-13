# Deployment Rehberi

## Hızlı Başlangıç

E-ticaret davranış analizi sistemini çalıştırmak için basit adımlar.

### Gereksinimler

- **Docker**: 20.10+
- **Docker Compose**: 2.0+
- **RAM**: 8GB+ (16GB önerilen)
- **CPU**: 4+ cores
- **Python**: 3.8+
- **Disk**: 20GB+ boş alan

## Demo Kurulumu

### 1. Servisleri Başlat
```bash
# Projeyi klonla
git clone <repository-url>
cd newmind-ai

# Environment değişkenlerini ayarla
cp .env.example .env
# .env dosyasını düzenle

# Servisleri başlat
docker-compose up -d
```

### 2. Demo Verisi Üret
```bash
# Demo verisi oluştur
python scripts/generate_ecommerce_data.py --size medium --output data/demo_data.jsonl

# Kafka'ya veri gönder
python scripts/send_demo_data.py --file data/demo_data.jsonl
```

### 3. Performans Testi
```bash
# Performans testini çalıştır
python scripts/ecommerce_performance_test.py --duration 300 --rate 100
```

### 4. Web Arayüzleri
- **Kafka UI**: http://localhost:8090
- **Qdrant Dashboard**: http://localhost:6333/dashboard
- **Grafana**: http://localhost:3000 (admin/admin123)
- **Spark UI**: http://localhost:8080
- **Health Check**: http://localhost:8080/health

### 5. Sistem Durumu
```bash
# Servisleri kontrol et
docker-compose ps

# Logları izle
docker-compose logs -f

# Belirli servis logları
docker-compose logs -f kafka
docker-compose logs -f qdrant

# Servisleri durdur
docker-compose down
```

## Production Deployment

### Kubernetes Deployment

#### 1. Namespace Oluştur
```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: vectorstream
```

```bash
kubectl apply -f namespace.yaml
```

#### 2. ConfigMap ve Secrets
```yaml
# configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: vectorstream-config
  namespace: vectorstream
data:
  kafka-config.yaml: |
    bootstrap_servers: "kafka-cluster:9092"
    topic: "ecommerce-events"
    group_id: "vectorstream-consumer"
  qdrant-config.yaml: |
    host: "qdrant-service"
    port: "6333"
    collection_name: "ecommerce_embeddings"
    vector_size: 384
```

```yaml
# secrets.yaml
apiVersion: v1
kind: Secret
metadata:
  name: vectorstream-secrets
  namespace: vectorstream
type: Opaque
data:
  api-key: <base64-encoded-api-key>
  kafka-password: <base64-encoded-password>
```

#### 3. Deployment Manifests
```yaml
# vectorstream-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vectorstream-app
  namespace: vectorstream
spec:
  replicas: 3
  selector:
    matchLabels:
      app: vectorstream
  template:
    metadata:
      labels:
        app: vectorstream
    spec:
      containers:
      - name: vectorstream
        image: vectorstream:latest
        ports:
        - containerPort: 8080
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          valueFrom:
            configMapKeyRef:
              name: vectorstream-config
              key: kafka-bootstrap-servers
        - name: QDRANT_HOST
          valueFrom:
            configMapKeyRef:
              name: vectorstream-config
              key: qdrant-host
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
```

#### 4. Service ve Ingress
```yaml
# service.yaml
apiVersion: v1
kind: Service
metadata:
  name: vectorstream-service
  namespace: vectorstream
spec:
  selector:
    app: vectorstream
  ports:
  - port: 80
    targetPort: 8080
  type: ClusterIP
```

```yaml
# ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: vectorstream-ingress
  namespace: vectorstream
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /
spec:
  rules:
  - host: vectorstream.yourdomain.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: vectorstream-service
            port:
              number: 80
```

### Docker Production Setup

#### 1. Production Docker Compose
```yaml
# docker-compose.prod.yml
version: '3.8'
services:
  vectorstream:
    build:
      context: .
      dockerfile: Dockerfile.prod
    ports:
      - "8080:8080"
    environment:
      - ENV=production
      - LOG_LEVEL=info
    volumes:
      - ./logs:/app/logs
      - ./data:/app/data
    depends_on:
      - kafka
      - qdrant
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.0'

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false'
    volumes:
      - kafka-data:/var/lib/kafka/data
    restart: unless-stopped

  qdrant:
    image: qdrant/qdrant:v1.7.0
    ports:
      - "6333:6333"
    volumes:
      - qdrant-data:/qdrant/storage
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: '2.0'

volumes:
  kafka-data:
  qdrant-data:
```

#### 2. Production Dockerfile
```dockerfile
# Dockerfile.prod
FROM python:3.11-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY src/ ./src/
COPY config/ ./config/
COPY scripts/ ./scripts/

# Create non-root user
RUN useradd -m -u 1000 vectorstream && \
    chown -R vectorstream:vectorstream /app
USER vectorstream

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

EXPOSE 8080

CMD ["python", "-m", "src.main"]
```

### Cloud Provider Deployment

#### AWS ECS Deployment
```json
{
  "family": "vectorstream-task",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "executionRoleArn": "arn:aws:iam::account:role/ecsTaskExecutionRole",
  "taskRoleArn": "arn:aws:iam::account:role/ecsTaskRole",
  "containerDefinitions": [
    {
      "name": "vectorstream",
      "image": "your-account.dkr.ecr.region.amazonaws.com/vectorstream:latest",
      "portMappings": [
        {
          "containerPort": 8080,
          "protocol": "tcp"
        }
      ],
      "environment": [
        {
          "name": "ENV",
          "value": "production"
        }
      ],
      "secrets": [
        {
          "name": "API_KEY",
          "valueFrom": "arn:aws:secretsmanager:region:account:secret:vectorstream/api-key"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/vectorstream",
          "awslogs-region": "us-west-2",
          "awslogs-stream-prefix": "ecs"
        }
      },
      "healthCheck": {
        "command": ["CMD-SHELL", "curl -f http://localhost:8080/health || exit 1"],
        "interval": 30,
        "timeout": 5,
        "retries": 3
      }
    }
  ]
}
```

## Monitoring ve Observability

### 1. Prometheus Monitoring
```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'vectorstream'
    static_configs:
      - targets: ['vectorstream:8080']
    metrics_path: '/metrics'
    scrape_interval: 10s

  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka:9092']
    
  - job_name: 'qdrant'
    static_configs:
      - targets: ['qdrant:6333']
```

### 2. Grafana Dashboards
```json
{
  "dashboard": {
    "title": "VectorStream Monitoring",
    "panels": [
      {
        "title": "Message Processing Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(kafka_messages_processed_total[5m])"
          }
        ]
      },
      {
        "title": "DLQ Error Rate",
        "type": "stat",
        "targets": [
          {
            "expr": "rate(dlq_messages_total[5m])"
          }
        ]
      },
      {
        "title": "System Resources",
        "type": "graph",
        "targets": [
          {
            "expr": "system_cpu_usage"
          },
          {
            "expr": "system_memory_usage"
          }
        ]
      }
    ]
  }
}
```

### 4. Log Aggregation (ELK Stack)
```yaml
# filebeat.yml
filebeat.inputs:
- type: log
  enabled: true
  paths:
    - /app/logs/*.log
  fields:
    service: vectorstream
  fields_under_root: true

output.elasticsearch:
  hosts: ["elasticsearch:9200"]
  index: "vectorstream-%{+yyyy.MM.dd}"

logging.level: info
```

## Security

### 1. API Security
```python
# API Key authentication
@app.middleware("http")
async def authenticate_request(request: Request, call_next):
    if request.url.path.startswith("/health"):
        response = await call_next(request)
        return response
    
    api_key = request.headers.get("X-API-Key")
    if not api_key or not validate_api_key(api_key):
        return JSONResponse(
            status_code=401,
            content={"error": "Invalid API key"}
        )
    
    response = await call_next(request)
    return response
```

### 2. Network Security
```yaml
# Network policies for Kubernetes
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: vectorstream-netpol
  namespace: vectorstream
spec:
  podSelector:
    matchLabels:
      app: vectorstream
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: ingress-nginx
    ports:
    - protocol: TCP
      port: 8080
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: kafka
    ports:
    - protocol: TCP
      port: 9092
```

### 3. Secrets Management
```bash
# Using Kubernetes secrets
kubectl create secret generic vectorstream-secrets \
  --from-literal=api-key=your-secret-key \
  --from-literal=kafka-password=your-kafka-password \
  --namespace=vectorstream

# Using AWS Secrets Manager
aws secretsmanager create-secret \
  --name vectorstream/api-key \
  --description "VectorStream API Key" \
  --secret-string "your-secret-key"
```

## Troubleshooting

### Yaygın Sorunlar

#### 1. Docker Servisleri Başlamıyor
```bash
# Portları kontrol et
sudo lsof -i :9092  # Kafka
sudo lsof -i :6333  # Qdrant
sudo lsof -i :8080  # Spark/App

# Docker loglarını kontrol et
docker-compose logs kafka
docker-compose logs qdrant

# Docker'ı yeniden başlat
docker-compose down -v
docker system prune -f
docker-compose up -d
```

#### 2. Kafka Bağlantı Hatası
```bash
# Kafka cluster durumunu kontrol et
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consumer group durumu
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Topic oluştur
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic ecommerce-events --partitions 3 --replication-factor 1
```

#### 3. Qdrant Veri Kaydetme Hatası
```bash
# Qdrant cluster info
curl -X GET "http://localhost:6333/cluster"

# Collection status
curl -X GET "http://localhost:6333/collections/ecommerce_embeddings"

# Collection recreate
curl -X DELETE "http://localhost:6333/collections/ecommerce_embeddings"
curl -X PUT "http://localhost:6333/collections/ecommerce_embeddings" \
  -H "Content-Type: application/json" \
  -d '{"vectors": {"size": 384, "distance": "Cosine"}}'
```

#### 4. High Memory Usage
```bash
# Container memory usage
docker stats

# Python memory profiling
pip install memory-profiler
python -m memory_profiler src/main.py

# Java heap dump (Kafka)
docker exec kafka jcmd 1 GC.run_finalization
docker exec kafka jmap -histo 1
```

#### 5. DLQ Issues
```bash
# DLQ stats
curl -X GET "http://localhost:8080/dlq/stats"

# Manual DLQ replay
curl -X POST "http://localhost:8080/dlq/replay/batch" \
  -H "Content-Type: application/json" \
  -d '{"message_ids": ["dlq_001"], "target_topic": "ecommerce-events"}'

# Clear DLQ files
rm -rf /data/dlq/*
```

### Performance Tuning

#### 1. Kafka Optimization
```properties
# server.properties
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
```

#### 2. Qdrant Optimization
```yaml
# qdrant-config.yaml
storage:
  storage_path: /qdrant/storage
  snapshots_path: /qdrant/snapshots

service:
  host: 0.0.0.0
  port: 6333
  grpc_port: 6334
  max_concurrent_collections: 10

cluster:
  enabled: false

telemetry:
  disabled: true
```

#### 3. Application Tuning
```python
# main.py optimization
import asyncio
import uvloop

# Use uvloop for better async performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Connection pooling
KAFKA_PRODUCER_CONFIG = {
    'bootstrap_servers': KAFKA_SERVERS,
    'batch_size': 16384,
    'linger_ms': 10,
    'buffer_memory': 33554432,
    'compression_type': 'snappy'
}

# Worker pool for CPU-bound tasks
from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=4)
```

### Load Testing

#### 1. Apache Bench
```bash
# Health endpoint load test
ab -n 1000 -c 10 http://localhost:8080/health

# DLQ stats endpoint
ab -n 500 -c 5 http://localhost:8080/dlq/stats
```

#### 2. Custom Load Test
```python
# load_test.py
import asyncio
import aiohttp
import time

async def load_test():
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(1000):
            task = session.get('http://localhost:8080/health')
            tasks.append(task)
        
        start_time = time.time()
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        end_time = time.time()
        
        success_count = sum(1 for r in responses if not isinstance(r, Exception))
        print(f"Completed {success_count}/1000 requests in {end_time - start_time:.2f}s")

asyncio.run(load_test())
```

## Backup ve Recovery

### 1. Data Backup
```bash
# Qdrant snapshot
curl -X POST "http://localhost:6333/collections/ecommerce_embeddings/snapshots"

# Kafka topic backup
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic ecommerce-events --from-beginning \
  --max-messages 1000000 > backup.jsonl

# DLQ backup
tar -czf dlq-backup-$(date +%Y%m%d).tar.gz /data/dlq/
```

### 2. Disaster Recovery
```bash
# Restore Qdrant collection
curl -X PUT "http://localhost:6333/collections/ecommerce_embeddings/snapshots/upload" \
  -F "snapshot=@snapshot_file.snapshot"

# Restore Kafka data
kafka-console-producer --bootstrap-server localhost:9092 \
  --topic ecommerce-events < backup.jsonl

# Restore DLQ
tar -xzf dlq-backup-20240115.tar.gz -C /
```

## Demo İçin Faydalı Bilgiler

### Servis Durumlarını Kontrol Etme
```bash
# Tüm servislerin durumunu kontrol et
docker-compose ps

# Belirli bir servisin loglarını görüntüle
docker-compose logs -f kafka
docker-compose logs -f qdrant
docker-compose logs -f spark-master

# Resource usage
docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"
```

### Performans İzleme
```bash
# Sistem kaynaklarını izle
docker stats

# Kafka topic metrics
docker exec kafka kafka-run-class kafka.tools.JmxTool \
  --object-name kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec

# Real-time health monitoring
watch -n 5 "curl -s http://localhost:8080/health | jq '.status'"
```

### Demo Sırasında Faydalı Komutlar
```bash
# Quick system overview
curl -s http://localhost:8080/health | jq
curl -s http://localhost:8080/dlq/stats | jq
curl -s http://localhost:8080/stats/performance | jq

# Generate load
python scripts/generate_load.py --rate 100 --duration 300

# Monitor in real-time
tail -f logs/app.log | grep ERROR
docker-compose logs -f | grep "ERROR\|WARN"
```
