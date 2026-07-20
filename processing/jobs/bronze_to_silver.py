"""Bronze -> Silver ETL.

Responsibilities:
  * clean and conform the raw batch tables (types, dedup, invalid rows out)
  * apply the late-arrival rule on streamed events: an event is accepted if it was
    ingested no later than 48 hours after its event_time, otherwise it is quarantined
    in silver.order_events_rejected (nothing is silently dropped)
  * upsert (MERGE) everything into the silver tables so the job is idempotent and
    late events update already-existing orders out of order
"""

from pyspark.sql import functions as F

from spark_config import get_spark, ensure_namespaces

LATE_LIMIT_HOURS = 48


def build_silver_customers(spark):
    # latest known state per customer across all loaded snapshots
    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.customers (
            customer_id INT, full_name STRING, email STRING, city STRING,
            segment STRING, signup_date DATE, updated_at TIMESTAMP
        ) USING iceberg
    """)
    spark.sql("""
        MERGE INTO silver.customers t
        USING (
            SELECT customer_id, full_name, email, city, segment, signup_date, updated_at
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY customer_id ORDER BY updated_at DESC, _ingested_at DESC
                ) AS rn
                FROM bronze.customers_raw
                WHERE customer_id IS NOT NULL
            ) WHERE rn = 1
        ) s
        ON t.customer_id = s.customer_id
        WHEN MATCHED AND s.updated_at >= t.updated_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("silver.customers:", spark.table("silver.customers").count())


def build_silver_products(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.products (
            product_id INT, product_name STRING, category STRING, unit_price DOUBLE
        ) USING iceberg
    """)
    spark.sql("""
        MERGE INTO silver.products t
        USING (
            SELECT product_id, product_name, category, unit_price
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY product_id ORDER BY _ingested_at DESC
                ) AS rn
                FROM bronze.products_raw
                WHERE product_id IS NOT NULL AND unit_price >= 0
            ) WHERE rn = 1
        ) s
        ON t.product_id = s.product_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("silver.products:", spark.table("silver.products").count())


def create_silver_order_tables(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.orders (
            order_id STRING, customer_id INT, product_id INT, quantity INT,
            unit_price DOUBLE, total_amount DOUBLE, status STRING,
            order_ts TIMESTAMP, source STRING, updated_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(order_ts))
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.order_events (
            event_id STRING, order_id STRING, customer_id INT, product_id INT,
            quantity INT, unit_price DOUBLE, status STRING,
            event_time TIMESTAMP, produced_at TIMESTAMP, _ingested_at TIMESTAMP,
            lateness_hours DOUBLE
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.order_events_rejected (
            event_id STRING, order_id STRING, customer_id INT, product_id INT,
            quantity INT, unit_price DOUBLE, status STRING,
            event_time TIMESTAMP, produced_at TIMESTAMP, _ingested_at TIMESTAMP,
            lateness_hours DOUBLE, reject_reason STRING, rejected_at TIMESTAMP
        ) USING iceberg
    """)


def merge_batch_orders(spark):
    # clean batch orders: valid keys, positive quantities/amounts, dedup on order_id
    spark.sql("""
        MERGE INTO silver.orders t
        USING (
            SELECT order_id, customer_id, product_id, quantity, unit_price,
                   total_amount, status, order_ts,
                   'batch' AS source, _ingested_at AS updated_at
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY order_id ORDER BY _ingested_at DESC
                ) AS rn
                FROM bronze.orders_raw
                WHERE order_id IS NOT NULL
                  AND customer_id IS NOT NULL
                  AND quantity > 0
                  AND total_amount >= 0
                  AND order_ts IS NOT NULL
            ) WHERE rn = 1
        ) s
        ON t.order_id = s.order_id
        WHEN MATCHED AND t.source = 'batch' THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("silver.orders after batch merge:", spark.table("silver.orders").count())


def split_events_by_lateness(spark):
    """Accept events ingested within 48h of event_time, quarantine the rest."""
    events = spark.table("bronze.order_events").withColumn(
        "lateness_hours",
        (F.unix_timestamp("_ingested_at") - F.unix_timestamp("event_time")) / 3600.0,
    )
    events.createOrReplaceTempView("staged_events")

    # accepted: within the allowed lateness window (idempotent merge on event_id)
    spark.sql(f"""
        MERGE INTO silver.order_events t
        USING (
            SELECT event_id, order_id, customer_id, product_id, quantity, unit_price,
                   status, event_time, produced_at, _ingested_at, lateness_hours
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY event_id ORDER BY _ingested_at DESC
                ) AS rn
                FROM staged_events
                WHERE event_id IS NOT NULL
                  AND order_id IS NOT NULL
                  AND event_time IS NOT NULL
                  AND lateness_hours <= {LATE_LIMIT_HOURS}
            ) WHERE rn = 1
        ) s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    # rejected: arrived more than 48h after event_time
    spark.sql(f"""
        MERGE INTO silver.order_events_rejected t
        USING (
            SELECT event_id, order_id, customer_id, product_id, quantity, unit_price,
                   status, event_time, produced_at, _ingested_at, lateness_hours,
                   'arrived more than {LATE_LIMIT_HOURS}h after event_time' AS reject_reason,
                   current_timestamp() AS rejected_at
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY event_id ORDER BY _ingested_at DESC
                ) AS rn
                FROM staged_events
                WHERE event_id IS NOT NULL
                  AND event_time IS NOT NULL
                  AND lateness_hours > {LATE_LIMIT_HOURS}
            ) WHERE rn = 1
        ) s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    acc = spark.table("silver.order_events").count()
    rej = spark.table("silver.order_events_rejected").count()
    print(f"silver.order_events: {acc} accepted, {rej} quarantined (> {LATE_LIMIT_HOURS}h late)")


def merge_stream_orders(spark):
    # collapse the accepted event stream to one row per order:
    # latest event wins for status/amounts, first event defines the order timestamp.
    # Late events therefore update orders that were already written in earlier runs.
    spark.sql("""
        MERGE INTO silver.orders t
        USING (
            SELECT e.order_id, e.customer_id, e.product_id, e.quantity, e.unit_price,
                   ROUND(e.quantity * e.unit_price, 2) AS total_amount, e.status,
                   f.first_event_time AS order_ts,
                   'stream' AS source, e._ingested_at AS updated_at
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY order_id ORDER BY event_time DESC, _ingested_at DESC
                ) AS rn
                FROM silver.order_events
            ) e
            JOIN (
                SELECT order_id, MIN(event_time) AS first_event_time
                FROM silver.order_events GROUP BY order_id
            ) f ON e.order_id = f.order_id
            WHERE e.rn = 1
        ) s
        ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("silver.orders after stream merge:", spark.table("silver.orders").count())


def main():
    spark = get_spark("bronze_to_silver")
    try:
        ensure_namespaces(spark)
        build_silver_customers(spark)
        build_silver_products(spark)
        create_silver_order_tables(spark)
        merge_batch_orders(spark)
        split_events_by_lateness(spark)
        merge_stream_orders(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
