# NewMind AI - Deployment Rehberi

## Genel Bakış

Bu rehber, NewMind AI sisteminin farklı ortamlarda nasıl deploy edileceğini açıklar. Sistem Docker containers ve Kubernetes orchestration kullanarak tasarlanmıştır.

## Ön Gereksinimler

### Sistem Gereksinimleri
- **CPU**: 8+ cores (16+ önerilen)
- **RAM**: 16GB+ (32GB+ önerilen)
- **Disk**: 100GB+ SSD
- **GPU**: NVIDIA GPU (RAPIDS için, opsiyonel)
- **Network**: 1Gbps+ bandwidth

### Yazılım Gereksinimleri
- Docker 20.10+
- Docker Compose 2.0+
- Kubernetes 1.24+ (production için)
- Helm 3.8+ (Kubernetes deployment için)
- kubectl (Kubernetes yönetimi için)

## Docker Deployment

### 1. Repository Klonlama
```bash
git clone https://github.com/your-org/newmind-ai.git
cd newmind-ai
```

### 2. Environment Konfigürasyonu
```bash
# .env dosyasını oluştur
cp .env.example .env

# Gerekli değişkenleri düzenle
vim .env
```

#### Önemli Environment Variables
```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_RAW_DATA=raw_data
KAFKA_TOPIC_DLQ=dead_letter_queue

# Qdrant Configuration
QDRANT_HOST=localhost
QDRANT_PORT=6333
QDRANT_COLLECTION_NAME=embeddings

# Spark Configuration
SPARK_MASTER=local[*]
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g

# RAPIDS Configuration (GPU)
RAPIDS_ENABLED=true
CUDA_VISIBLE_DEVICES=0

# Monitoring
PROMETHEUS_PORT=9090
GRAFANA_PORT=3000

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
```

### 3. Docker Compose ile Başlatma
```bash
# Tüm servisleri başlat
docker-compose up -d

# Logları izle
docker-compose logs -f

# Servis durumunu kontrol et
docker-compose ps
```

### 4. Health Check
```bash
# Sistem sağlığını kontrol et
curl http://localhost:8000/health

# Kafka sağlığını kontrol et
curl http://localhost:8000/health/kafka

# Qdrant sağlığını kontrol et
curl http://localhost:8000/health/qdrant
```

### 5. Test Mesajı Gönderme
```bash
# Test verisi gönder
python send_test_message.py

# Veya manuel olarak
curl -X POST http://localhost:9092/topics/raw_data \
  -H "Content-Type: application/json" \
  -d '{"text": "Test message for embedding", "id": "test-001"}'
```

## Kubernetes Deployment

### 1. Namespace Oluşturma
```bash
# Namespace'leri oluştur
kubectl apply -f deployment/k8s/namespace.yaml
```

### 2. Secrets ve ConfigMaps
```bash
# Secrets oluştur
kubectl create secret generic newmind-secrets \
  --from-literal=kafka-password=your-kafka-password \
  --from-literal=qdrant-api-key=your-qdrant-key \
  -n newmind-ai

# ConfigMap oluştur
kubectl create configmap app-config \
  --from-file=config/app_config.yaml \
  -n newmind-ai
```

### 3. Persistent Volumes
```bash
# Storage class kontrol et
kubectl get storageclass

# PV'leri oluştur (eğer dynamic provisioning yoksa)
kubectl apply -f deployment/k8s/storage/
```

### 4. Infrastructure Services
```bash
# Kafka StatefulSet
kubectl apply -f deployment/k8s/kafka/

# Qdrant StatefulSet
kubectl apply -f deployment/k8s/qdrant/

# Monitoring stack
kubectl apply -f deployment/k8s/monitoring/
```

### 5. Application Deployment
```bash
# Application services
kubectl apply -f deployment/k8s/deployment.yaml
kubectl apply -f deployment/k8s/service.yaml

# Ingress (opsiyonel)
kubectl apply -f deployment/k8s/ingress.yaml
```

### 6. Helm ile Deployment (Önerilen)
```bash
# Helm chart'ı yükle
helm install newmind-ai deployment/helm/ \
  --namespace newmind-ai \
  --create-namespace \
  --values deployment/helm/values.yaml

# Upgrade
helm upgrade newmind-ai deployment/helm/ \
  --namespace newmind-ai

# Status kontrol
helm status newmind-ai -n newmind-ai
```

## Production Konfigürasyonu

### 1. Resource Limits
```yaml
# deployment.yaml örneği
resources:
  requests:
    memory: "2Gi"
    cpu: "1000m"
  limits:
    memory: "4Gi"
    cpu: "2000m"
    nvidia.com/gpu: 1  # GPU için
```

### 2. Horizontal Pod Autoscaler
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: newmind-ai-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: kafka-consumer
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

### 3. Network Policies
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: newmind-ai-network-policy
spec:
  podSelector:
    matchLabels:
      app: newmind-ai
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: newmind-ai
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: infrastructure
```

### 4. Security Context
```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  runAsGroup: 1000
  fsGroup: 1000
  capabilities:
    drop:
    - ALL
  readOnlyRootFilesystem: true
  allowPrivilegeEscalation: false
```

## Monitoring ve Logging

### 1. Prometheus Konfigürasyonu
```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'newmind-ai'
    kubernetes_sd_configs:
    - role: pod
      namespaces:
        names:
        - newmind-ai
    relabel_configs:
    - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
      action: keep
      regex: true
```

### 2. Grafana Dashboard Import
```bash
# Dashboard'ları import et
kubectl apply -f monitoring/grafana/dashboards/

# Grafana'ya erişim
kubectl port-forward svc/grafana 3000:3000 -n monitoring
```

### 3. Log Aggregation (ELK Stack)
```bash
# Elasticsearch, Logstash, Kibana deploy et
kubectl apply -f deployment/k8s/logging/

# Fluentd DaemonSet
kubectl apply -f deployment/k8s/logging/fluentd-daemonset.yaml
```

## Backup ve Recovery

### 1. Kafka Backup
```bash
# Kafka topics backup
kafka-topics.sh --bootstrap-server localhost:9092 --list > topics.txt

# Consumer group offsets backup
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list > consumer-groups.txt
```

### 2. Qdrant Backup
```bash
# Qdrant snapshot oluştur
curl -X POST "http://localhost:6333/collections/embeddings/snapshots"

# Snapshot'ı S3'e yükle
aws s3 cp /qdrant/snapshots/ s3://your-backup-bucket/qdrant/ --recursive
```

### 3. Configuration Backup
```bash
# ConfigMaps ve Secrets backup
kubectl get configmaps -o yaml > configmaps-backup.yaml
kubectl get secrets -o yaml > secrets-backup.yaml
```

## Troubleshooting

### 1. Yaygın Sorunlar

#### Kafka Bağlantı Sorunları
```bash
# Kafka broker'ların durumunu kontrol et
kubectl logs -l app=kafka -n infrastructure

# Network connectivity test
kubectl exec -it kafka-consumer-pod -- nc -zv kafka-service 9092
```

#### Qdrant Bağlantı Sorunları
```bash
# Qdrant service durumu
kubectl get svc qdrant-service -n infrastructure

# Qdrant health check
curl http://qdrant-service:6333/health
```

#### GPU Sorunları
```bash
# NVIDIA device plugin kontrol
kubectl get daemonset nvidia-device-plugin-daemonset -n kube-system

# GPU resources kontrol
kubectl describe node | grep nvidia.com/gpu

# Pod GPU allocation
kubectl describe pod embedding-processor-pod | grep nvidia.com/gpu
```

### 2. Performance Tuning

#### Kafka Optimization
```bash
# Kafka broker konfigürasyonu
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
```

#### Spark Optimization
```bash
# Spark executor konfigürasyonu
spark.executor.instances=4
spark.executor.cores=4
spark.executor.memory=8g
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```

#### Qdrant Optimization
```yaml
# Qdrant konfigürasyonu
storage:
  optimizers:
    deleted_threshold: 0.2
    vacuum_min_vector_number: 1000
    default_segment_number: 0
```

### 3. Log Analysis

#### Application Logs
```bash
# Tüm pod logları
kubectl logs -l app=newmind-ai --tail=100 -f

# Specific component logs
kubectl logs deployment/kafka-consumer --tail=100
kubectl logs deployment/embedding-processor --tail=100
kubectl logs deployment/qdrant-writer --tail=100
```

#### Error Pattern Analysis
```bash
# Error loglarını filtrele
kubectl logs -l app=newmind-ai | grep -i error

# Circuit breaker durumları
kubectl logs -l app=newmind-ai | grep "circuit.*breaker"

# Retry attempts
kubectl logs -l app=newmind-ai | grep "retry"
```

## Güvenlik Best Practices

### 1. Image Security
```bash
# Image vulnerability scan
docker scan newmind-ai:latest

# Multi-stage build kullan
# Base image'ları minimal tut (alpine, distroless)
```

### 2. Network Security
```bash
# TLS certificates
kubectl create secret tls newmind-ai-tls \
  --cert=path/to/tls.crt \
  --key=path/to/tls.key

# Service mesh (Istio) konfigürasyonu
kubectl label namespace newmind-ai istio-injection=enabled
```

### 3. RBAC
```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: newmind-ai-role
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps"]
  verbs: ["get", "list", "watch"]
```

## Maintenance

### 1. Rolling Updates
```bash
# Deployment update
kubectl set image deployment/kafka-consumer \
  kafka-consumer=newmind-ai:v2.0.0

# Rollback
kubectl rollout undo deployment/kafka-consumer

# Update status
kubectl rollout status deployment/kafka-consumer
```

### 2. Database Maintenance
```bash
# Qdrant collection optimization
curl -X POST "http://localhost:6333/collections/embeddings/index"

# Kafka log cleanup
kafka-log-dirs.sh --bootstrap-server localhost:9092 --describe
```

### 3. Monitoring Alerts
```yaml
# Prometheus alert rules
groups:
- name: newmind-ai
  rules:
  - alert: HighErrorRate
    expr: rate(http_requests_total{status=~"5.."}[5m]) > 0.1
    for: 5m
    annotations:
      summary: "High error rate detected"
```

## Disaster Recovery Plan

### 1. Backup Schedule
- **Daily**: Application data, configurations
- **Weekly**: Full system backup
- **Monthly**: Long-term archive

### 2. Recovery Procedures
1. **RTO (Recovery Time Objective)**: 15 minutes
2. **RPO (Recovery Point Objective)**: 5 minutes
3. **Automated failover**: Health check based
4. **Manual intervention**: Critical failures

### 3. Testing
- **Monthly**: Backup restore test
- **Quarterly**: Full disaster recovery drill
- **Annually**: Business continuity test