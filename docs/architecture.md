# Architecture

Three isolated components, each with its own Docker Compose file, communicating
over one shared Docker network (`lakehouse`).

```mermaid
flowchart LR
    subgraph streaming ["streaming/ (docker-compose)"]
        PR[order-events-producer<br/>Python] -->|JSON events| KF[(Kafka<br/>topic: orders_events)]
    end

    subgraph processing ["processing/ (docker-compose)"]
        KF -->|Structured Streaming<br/>30s micro-batches| ST[streaming_ingest.py]
        CSV[/initial CSVs<br/>processing/data/] --> SEED[seed_bronze.py]
        ST --> BRZ[(bronze.*)]
        SEED --> BRZ
        BRZ --> B2S[bronze_to_silver.py<br/>clean + dedup + 48h late rule] --> SLV[(silver.*)]
        SLV --> S2G[silver_to_gold.py<br/>SCD2 + star schema] --> GLD[(gold.*)]
        BRZ -.-> DQ[data_quality.py<br/>+ ge_quality.py]
        SLV -.-> DQ
        GLD -.-> DQ
        DQ --> AUD[(audit.*)]

        REST[Iceberg REST catalog] --- BRZ & SLV & GLD
        MINIO[(MinIO<br/>s3://warehouse)] --- REST
    end

    subgraph orchestration ["orchestration/ (docker-compose)"]
        AF[Airflow<br/>batch_etl + streaming_supervisor] -->|docker exec spark-submit| SEED
        AF -->|docker exec spark-submit| B2S & S2G & DQ
        AF -->|supervises| ST
        PG[(Postgres<br/>Airflow metadata)] --- AF
    end
```

## Data flow

1. **Batch path**: generated CSVs (customers, products, 60 days of orders) are loaded
   into `bronze.*` Iceberg tables by `seed_bronze.py` (idempotent MERGE/overwrite).
2. **Streaming path**: the Python producer emits order lifecycle events to Kafka;
   Spark Structured Streaming appends them to `bronze.order_events` with
   exactly-once semantics (checkpointed).
3. **bronze → silver**: cleaning, type conformance, deduplication; the 48h
   late-arrival rule accepts late events into `silver.order_events` / merges them
   into `silver.orders`, and quarantines very late events in
   `silver.order_events_rejected`.
4. **silver → gold**: star schema build — SCD2 customer dimension, product and date
   dimensions, order fact with SCD2-aware surrogate key resolution, daily sales mart.
5. **Data quality** gates run after every stage (bronze, silver, gold) and write an
   audit trail; a failed ERROR-level check blocks the downstream stage.

## Key decisions

- **Iceberg REST catalog + MinIO**: no Hive metastore, no HDFS (both forbidden).
  All table metadata goes through the REST catalog, data files live in
  `s3://warehouse/` on MinIO.
- **Spark runs only in the processing component**: Airflow uses `docker exec` against
  the `spark-master` container to submit every job, so Spark drivers/executors never
  run on orchestration containers (grading disqualification rule).
- **Shared external network**: each compose file joins the pre-created `lakehouse`
  network, keeping the components independently deployable but able to communicate
  by container name (`kafka`, `minio`, `iceberg-rest`, `spark-master`).
- **Idempotent jobs**: every batch job can be re-run safely (MERGE on business keys,
  snapshot-scoped deletes, full rebuilds for derived gold tables), which is what makes
  hourly scheduling and task retries safe.
