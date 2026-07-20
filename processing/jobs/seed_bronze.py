"""Loads the batch source files (CSV) into bronze Iceberg tables.

Usage:
    spark-submit seed_bronze.py [day1|day2]

day1 (default) loads the initial customer snapshot, day2 loads the later snapshot
that contains changed customers (used to demonstrate the SCD2 dimension).
Products and orders are full historical loads and are idempotent (overwrite).
Customer snapshots are idempotent per snapshot label (delete + insert).
"""

import sys

from pyspark.sql import functions as F

from spark_config import get_spark, ensure_namespaces

DATA_DIR = "/opt/spark/data"


def load_customers(spark, snapshot: str) -> None:
    df = (
        spark.read.option("header", True)
        .csv(f"{DATA_DIR}/customers_{snapshot}.csv")
        .select(
            F.col("customer_id").cast("int"),
            "full_name", "email", "city", "segment",
            F.col("signup_date").cast("date"),
            F.col("updated_at").cast("timestamp"),
        )
        .withColumn("snapshot", F.lit(snapshot))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.customers_raw (
            customer_id INT, full_name STRING, email STRING, city STRING,
            segment STRING, signup_date DATE, updated_at TIMESTAMP,
            snapshot STRING, _ingested_at TIMESTAMP
        ) USING iceberg
    """)
    spark.sql(f"DELETE FROM bronze.customers_raw WHERE snapshot = '{snapshot}'")
    df.writeTo("bronze.customers_raw").append()
    print(f"bronze.customers_raw <- snapshot {snapshot}: {df.count()} rows")


def load_products(spark) -> None:
    df = (
        spark.read.option("header", True)
        .csv(f"{DATA_DIR}/products.csv")
        .select(
            F.col("product_id").cast("int"),
            "product_name", "category",
            F.col("unit_price").cast("double"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.writeTo("bronze.products_raw").using("iceberg").createOrReplace()
    print(f"bronze.products_raw <- {df.count()} rows")


def load_orders(spark) -> None:
    df = (
        spark.read.option("header", True)
        .csv(f"{DATA_DIR}/orders.csv")
        .select(
            "order_id",
            F.col("customer_id").cast("int"),
            F.col("product_id").cast("int"),
            F.col("quantity").cast("int"),
            F.col("unit_price").cast("double"),
            F.col("total_amount").cast("double"),
            "status",
            F.col("order_ts").cast("timestamp"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.writeTo("bronze.orders_raw").using("iceberg").createOrReplace()
    print(f"bronze.orders_raw <- {df.count()} rows")


def create_order_events_table(spark) -> None:
    # target of the streaming job; created here so the batch DAG can run DQ on it
    # even before the first streaming micro-batch lands
    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.order_events (
            event_id STRING, order_id STRING, customer_id INT, product_id INT,
            quantity INT, unit_price DOUBLE, status STRING,
            event_time TIMESTAMP, produced_at TIMESTAMP, _ingested_at TIMESTAMP,
            kafka_partition INT, kafka_offset BIGINT
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """)


def main():
    snapshot = sys.argv[1] if len(sys.argv) > 1 else "day1"
    if snapshot not in ("day1", "day2"):
        raise SystemExit(f"unknown snapshot '{snapshot}', expected day1 or day2")

    spark = get_spark(f"seed_bronze_{snapshot}")
    try:
        ensure_namespaces(spark)
        load_customers(spark, snapshot)
        load_products(spark)
        load_orders(spark)
        create_order_events_table(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
