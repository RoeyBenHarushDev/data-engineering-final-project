"""Spark Structured Streaming job: Kafka topic `orders_events` -> bronze.order_events.

Bronze keeps every event exactly as produced (plus ingestion metadata) — no filtering
here; the 48h late-arrival rule is applied in bronze_to_silver.py where events are
either accepted or quarantined. Checkpointing makes the ingestion exactly-once into
Iceberg.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

from spark_config import get_spark, ensure_namespaces

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "orders_events"
CHECKPOINT = "/opt/spark/checkpoints/order_events"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("status", StringType()),
    StructField("event_time", TimestampType()),
    StructField("produced_at", TimestampType()),
])


def main():
    spark = get_spark("streaming_ingest_order_events")
    ensure_namespaces(spark)

    # make sure the target table exists (same DDL as seed_bronze.py)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.order_events (
            event_id STRING, order_id STRING, customer_id INT, product_id INT,
            quantity INT, unit_price DOUBLE, status STRING,
            event_time TIMESTAMP, produced_at TIMESTAMP, _ingested_at TIMESTAMP,
            kafka_partition INT, kafka_offset BIGINT
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    events = (
        raw.select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .select("e.*", "kafka_partition", "kafka_offset")
        .filter(F.col("event_id").isNotNull())  # drop unparseable payloads
        .withColumn("_ingested_at", F.current_timestamp())
    )

    query = (
        events.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", CHECKPOINT)
        .toTable("bronze.order_events")
    )
    print(f"streaming from kafka://{KAFKA_BOOTSTRAP}/{TOPIC} into bronze.order_events")
    query.awaitTermination()


if __name__ == "__main__":
    main()
