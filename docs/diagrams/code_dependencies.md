```mermaid
flowchart TD
    %% ---------- CORE (real-time) ----------
    subgraph Core
        style Core fill:#F9F9F9,stroke:#999,stroke-width:1
        KC[kafka_consumer.py]
        EP[embedding_processor.py]
        QW[qdrant_writer.py]
        PO[pipeline_orchestrator.py]
        KC -->|events| EP
        EP -->|vectors| QW
        PO --> KC
        PO --> EP
        PO --> QW
    end

    %% ---------- SPARK (distributed) ----------
    subgraph Spark
        style Spark fill:#F0F8FF,stroke:#339,stroke-width:1
        KSC[kafka_spark_connector.py]
        SEJ[embedding_job.py]
        DEP[distributed_embedding_processor.py]
        KSC --> SEJ
        SEJ --> DEP
        DEP -->|vectors| QW
    end

    %% ---------- OPTIONAL LAYERS ----------
    subgraph Optional Layers
        style Optional fill:#FFF7F0,stroke:#C96,stroke-width:1,stroke-dasharray: 3
        GPU[gpu_enhanced_embedding_job.py<br/>rapids_gpu_processor.py]
        Batch[batch_processor.py<br/>optimized_batch_processor.py]
        GPU -.-> SEJ
        Batch -.-> SEJ
    end

    %% ---------- SHARED TARGET ----------
    QW -->|writes| Qdrant[(Qdrant DB)]
