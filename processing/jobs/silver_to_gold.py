"""Silver -> Gold ETL: star schema.

  gold.dim_customer  - Type 2 SCD (history kept with effective_from/effective_to/is_current)
  gold.dim_product   - Type 1 SCD (overwrite in place)
  gold.dim_date      - generated calendar dimension
  gold.fact_orders   - order-grain fact, surrogate keys resolved against the SCD2 dimension
  gold.daily_sales   - aggregate mart on top of the fact
"""

from spark_config import get_spark, ensure_namespaces


def build_dim_customer_scd2(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.dim_customer (
            customer_sk BIGINT, customer_id INT, full_name STRING, email STRING,
            city STRING, segment STRING, signup_date DATE,
            effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN
        ) USING iceberg
    """)

    # rows that are new or differ from the current dimension version
    changes = spark.sql("""
        SELECT s.customer_id, s.full_name, s.email, s.city, s.segment,
               s.signup_date, s.updated_at
        FROM silver.customers s
        LEFT JOIN gold.dim_customer d
               ON d.customer_id = s.customer_id AND d.is_current = true
        WHERE d.customer_id IS NULL
           OR d.full_name <> s.full_name OR d.email <> s.email
           OR d.city <> s.city OR d.segment <> s.segment
    """)
    changes.cache()
    n_changes = changes.count()  # materialize BEFORE mutating the dimension
    changes.createOrReplaceTempView("scd2_changes")
    print(f"dim_customer SCD2: {n_changes} new/changed customers")
    if n_changes == 0:
        changes.unpersist()
        return

    # step 1: close the current version of changed customers
    spark.sql("""
        MERGE INTO gold.dim_customer d
        USING scd2_changes s
        ON d.customer_id = s.customer_id AND d.is_current = true
        WHEN MATCHED THEN UPDATE SET d.is_current = false, d.effective_to = s.updated_at
    """)

    # step 2: open a new version for every new/changed customer
    spark.sql("""
        INSERT INTO gold.dim_customer
        SELECT m.max_sk + ROW_NUMBER() OVER (ORDER BY s.customer_id) AS customer_sk,
               s.customer_id, s.full_name, s.email, s.city, s.segment, s.signup_date,
               s.updated_at AS effective_from,
               CAST(NULL AS TIMESTAMP) AS effective_to,
               true AS is_current
        FROM scd2_changes s
        CROSS JOIN (SELECT COALESCE(MAX(customer_sk), 0) AS max_sk
                    FROM gold.dim_customer) m
    """)
    changes.unpersist()
    total = spark.table("gold.dim_customer").count()
    current = spark.sql("SELECT COUNT(*) c FROM gold.dim_customer WHERE is_current").first()["c"]
    print(f"gold.dim_customer: {total} rows total, {current} current")


def build_dim_product(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.dim_product (
            product_sk INT, product_id INT, product_name STRING,
            category STRING, unit_price DOUBLE
        ) USING iceberg
    """)
    # Type 1: attributes are overwritten, no history (product_sk == product_id, documented)
    spark.sql("""
        MERGE INTO gold.dim_product t
        USING (SELECT product_id AS product_sk, product_id, product_name,
                      category, unit_price
               FROM silver.products) s
        ON t.product_id = s.product_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("gold.dim_product:", spark.table("gold.dim_product").count())


def build_dim_date(spark):
    spark.sql("""
        CREATE OR REPLACE TABLE gold.dim_date USING iceberg AS
        SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
               d AS full_date,
               year(d)                     AS year,
               quarter(d)                  AS quarter,
               month(d)                    AS month,
               day(d)                      AS day,
               date_format(d, 'EEEE')      AS day_name,
               dayofweek(d) IN (6, 7)      AS is_weekend
        FROM (SELECT explode(sequence(DATE'2026-01-01', DATE'2026-12-31',
                                      INTERVAL 1 DAY)) AS d)
    """)
    print("gold.dim_date:", spark.table("gold.dim_date").count())


def build_fact_orders(spark):
    # SCD2-aware surrogate key lookup: pick the customer version whose validity window
    # covers the order timestamp; orders older than the first known version fall back
    # to the earliest version of that customer.
    spark.sql("""
        CREATE OR REPLACE TABLE gold.fact_orders USING iceberg
        PARTITIONED BY (days(order_ts)) AS
        SELECT order_id, date_key, customer_sk, product_sk, quantity, unit_price,
               total_amount, status, order_ts, source
        FROM (
            SELECT o.order_id,
                   CAST(date_format(o.order_ts, 'yyyyMMdd') AS INT) AS date_key,
                   d.customer_sk,
                   COALESCE(p.product_sk, -1) AS product_sk,
                   o.quantity, o.unit_price, o.total_amount, o.status,
                   o.order_ts, o.source,
                   ROW_NUMBER() OVER (
                       PARTITION BY o.order_id
                       ORDER BY CASE WHEN o.order_ts >= d.effective_from
                                      AND o.order_ts < COALESCE(d.effective_to,
                                                                TIMESTAMP'9999-12-31')
                                     THEN 0 ELSE 1 END,
                                d.effective_from
                   ) AS pick
            FROM silver.orders o
            JOIN gold.dim_customer d ON d.customer_id = o.customer_id
            LEFT JOIN gold.dim_product p ON p.product_id = o.product_id
        ) WHERE pick = 1
    """)
    print("gold.fact_orders:", spark.table("gold.fact_orders").count())


def build_daily_sales(spark):
    spark.sql("""
        CREATE OR REPLACE TABLE gold.daily_sales USING iceberg AS
        SELECT f.date_key,
               p.category,
               COUNT(DISTINCT f.order_id)  AS orders_count,
               SUM(f.quantity)             AS units_sold,
               ROUND(SUM(f.total_amount), 2) AS revenue
        FROM gold.fact_orders f
        LEFT JOIN gold.dim_product p ON p.product_sk = f.product_sk
        WHERE f.status IN ('completed', 'shipped')
        GROUP BY f.date_key, p.category
    """)
    print("gold.daily_sales:", spark.table("gold.daily_sales").count())


def main():
    spark = get_spark("silver_to_gold")
    try:
        ensure_namespaces(spark)
        build_dim_customer_scd2(spark)
        build_dim_product(spark)
        build_dim_date(spark)
        build_fact_orders(spark)
        build_daily_sales(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
