# System Diagrams

Bu klasör NewMind AI projesinin sistem mimarisini gösteren Mermaid diyagramlarını içerir.

## Diyagramlar

### 1. System Architecture (`system_architecture.md`)
Sistem bileşenlerinin genel mimarisini gösterir:
- Kafka cluster ve consumer yapısı
- Spark/RAPIDS processing layer
- Qdrant vector database
- Monitoring ve logging bileşenleri
- Configuration management

### 2. Data Flow (`data_flow.md`)
Veri akışının sequence diagram'ını gösterir:
- Data ingestion süreçleri
- Message processing pipeline
- Error handling flows
- Monitoring ve metrics collection

### 3. Deployment Architecture (`deployment_architecture.md`)
Kubernetes deployment mimarisini gösterir:
- Pod ve service yapısı
- Namespace organizasyonu
- Persistent storage
- Load balancing
- External services

### 4. Error Handling Flow (`error_handling_flow.md`)
Error handling stratejisini gösterir:
- Retry mechanisms
- Circuit breaker pattern
- Dead letter queues
- Alert escalation
- Graceful degradation

## Diyagramları Görüntüleme

### Online Mermaid Editor
1. [Mermaid Live Editor](https://mermaid.live/) adresine gidin
2. `.md` dosyasının içeriğini kopyalayın
3. Editor'a yapıştırın
4. Diyagram otomatik olarak render edilecek

### VS Code Extension
1. "Mermaid Preview" extension'ını yükleyin
2. `.md` dosyasını açın
3. `Ctrl+Shift+P` ile command palette'i açın
4. "Mermaid: Preview" komutunu çalıştırın

### GitHub/GitLab
GitHub ve GitLab otomatik olarak `.md` dosyalarını render eder.

### PNG/SVG Export
Mermaid Live Editor'dan PNG veya SVG formatında export edebilirsiniz:
1. Diyagramı Mermaid Live Editor'da açın
2. "Actions" menüsünden "Download PNG" veya "Download SVG" seçin

## Diyagram Güncelleme

Diyagramları güncellemek için:
1. İlgili `.md` dosyasını düzenleyin
2. Mermaid syntax'ına uygun olduğundan emin olun
3. Değişiklikleri test edin
4. Gerekirse PNG/SVG export'larını güncelleyin

## Mermaid Syntax Referansı

- [Flowchart](https://mermaid.js.org/syntax/flowchart.html)
- [Sequence Diagram](https://mermaid.js.org/syntax/sequenceDiagram.html)
- [Class Diagram](https://mermaid.js.org/syntax/classDiagram.html)
- [State Diagram](https://mermaid.js.org/syntax/stateDiagram.html)