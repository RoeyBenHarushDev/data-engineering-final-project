# Components

## processing/ — lakehouse + Spark

| Service | Image | Purpose |
|---|---|---|
| minio | minio/minio | S3-compatible object store, bucket `warehouse` (console: http://localhost:9001, admin/password) |
| minio-init | minio/mc | one-shot bucket creation |
| iceberg-rest | tabulario/iceberg-rest | Iceberg REST catalog backed by MinIO |
| spark-master | custom (tabulario/spark-iceberg + Kafka jars + Great Expectations) | the ONLY container where Spark applications run |

Jobs in `processing/jobs/` (all submitted with `spark-submit` inside spark-master):

- `generate_initial_data.py` — regenerates the committed CSVs (plain Python, deterministic seed)
- `seed_bronze.py [day1|day2]` — batch CSVs → bronze Iceberg tables
- `streaming_ingest.py` — Kafka → `bronze.order_events` (Structured Streaming, 30s trigger, checkpointed)
- `bronze_to_silver.py` — cleaning, dedup, 48h late-arrival split, MERGEs into silver
- `silver_to_gold.py` — SCD2 customer dimension, product/date dimensions, order fact, daily sales
- `data_quality.py <stage>` — native DQ gate per layer, audit trail in `audit.dq_results`
- `ge_quality.py` — Great Expectations suite (bonus), audit trail in `audit.ge_results`
- `show_tables.py [layer]` — prints counts + samples for the demo

## streaming/ — Kafka + producer

| Service | Image | Purpose |
|---|---|---|
| kafka | apache/kafka (KRaft, no ZooKeeper) | broker, topic `orders_events` (3 partitions) |
| kafka-init | apache/kafka | one-shot topic creation |
| order-events-producer | custom python:3.11-slim | emits order lifecycle events as JSON; configurable rate and late-event ratios via environment variables |

## orchestration/ — Airflow

| Service | Image | Purpose |
|---|---|---|
| airflow-db | postgres:16-alpine | Airflow metadata DB |
| airflow-init | custom (apache/airflow + docker CLI) | one-shot migration + admin user |
| airflow-webserver | same | UI at http://localhost:8090 (admin/admin) |
| airflow-scheduler | same | schedules the DAGs |

DAGs:

- `batch_etl` (@hourly): load_batch_sources → dq_bronze → bronze_to_silver →
  dq_silver → silver_to_gold → dq_gold → great_expectations_validation.
  Retries (2x, 2 min delay) and a failure alert callback on every task.
  Param `snapshot` (day1/day2) selects the customer snapshot for the SCD2 demo.
- `streaming_supervisor` (*/10 min): checks the processing container and starts the
  streaming ingestion inside spark-master if it is not already running.

Airflow talks to Spark exclusively through `docker exec spark-master spark-submit ...`
(docker socket mounted read-write into the Airflow containers), so Spark never runs
on orchestration containers.
