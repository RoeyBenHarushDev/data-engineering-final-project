"""Data quality gate, run by Airflow after each pipeline stage.

Usage:
    spark-submit data_quality.py <bronze|silver|gold>

Every check is a SQL statement returning a violation count (0 = pass). Results are
persisted to audit.dq_results for monitoring. Checks with severity ERROR fail the
job (non-zero exit) which fails the Airflow task and blocks downstream stages;
WARN checks are recorded but do not block (bronze is raw by design, so dirty rows
there are expected and only reported).
"""

import sys

from spark_config import get_spark, ensure_namespaces

# (name, severity, sql returning a single column `violations`)
CHECKS = {
    "bronze": [
        ("orders_raw_not_empty", "ERROR",
         "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS violations FROM bronze.orders_raw"),
        ("customers_raw_not_empty", "ERROR",
         "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS violations FROM bronze.customers_raw"),
        ("products_raw_not_empty", "ERROR",
         "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS violations FROM bronze.products_raw"),
        ("orders_raw_null_keys", "WARN",
         "SELECT COUNT(*) AS violations FROM bronze.orders_raw "
         "WHERE order_id IS NULL OR customer_id IS NULL"),
        ("orders_raw_duplicate_ids", "WARN",
         "SELECT COALESCE(SUM(c - 1), 0) AS violations FROM "
         "(SELECT order_id, COUNT(*) c FROM bronze.orders_raw GROUP BY order_id HAVING COUNT(*) > 1)"),
        ("orders_raw_negative_values", "WARN",
         "SELECT COUNT(*) AS violations FROM bronze.orders_raw "
         "WHERE quantity <= 0 OR total_amount < 0"),
        ("order_events_null_event_id", "WARN",
         "SELECT COUNT(*) AS violations FROM bronze.order_events WHERE event_id IS NULL"),
    ],
    "silver": [
        ("orders_not_empty", "ERROR",
         "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS violations FROM silver.orders"),
        ("orders_unique_order_id", "ERROR",
         "SELECT COALESCE(SUM(c - 1), 0) AS violations FROM "
         "(SELECT order_id, COUNT(*) c FROM silver.orders GROUP BY order_id HAVING COUNT(*) > 1)"),
        ("orders_no_null_keys", "ERROR",
         "SELECT COUNT(*) AS violations FROM silver.orders "
         "WHERE order_id IS NULL OR customer_id IS NULL"),
        ("orders_positive_quantity", "ERROR",
         "SELECT COUNT(*) AS violations FROM silver.orders WHERE quantity <= 0"),
        ("orders_non_negative_amount", "ERROR",
         "SELECT COUNT(*) AS violations FROM silver.orders WHERE total_amount < 0"),
        ("orders_customer_exists", "ERROR",
         "SELECT COUNT(*) AS violations FROM silver.orders o "
         "LEFT ANTI JOIN silver.customers c ON o.customer_id = c.customer_id"),
        ("order_events_unique_event_id", "ERROR",
         "SELECT COALESCE(SUM(c - 1), 0) AS violations FROM "
         "(SELECT event_id, COUNT(*) c FROM silver.order_events GROUP BY event_id HAVING COUNT(*) > 1)"),
        ("order_events_within_48h", "ERROR",
         "SELECT COUNT(*) AS violations FROM silver.order_events WHERE lateness_hours > 48"),
    ],
    "gold": [
        ("fact_orders_not_empty", "ERROR",
         "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS violations FROM gold.fact_orders"),
        ("fact_orders_unique_order_id", "ERROR",
         "SELECT COALESCE(SUM(c - 1), 0) AS violations FROM "
         "(SELECT order_id, COUNT(*) c FROM gold.fact_orders GROUP BY order_id HAVING COUNT(*) > 1)"),
        ("dim_customer_single_current_version", "ERROR",
         "SELECT COUNT(*) AS violations FROM "
         "(SELECT customer_id FROM gold.dim_customer WHERE is_current "
         "GROUP BY customer_id HAVING COUNT(*) > 1)"),
        ("fact_orders_valid_customer_sk", "ERROR",
         "SELECT COUNT(*) AS violations FROM gold.fact_orders f "
         "LEFT ANTI JOIN gold.dim_customer d ON f.customer_sk = d.customer_sk"),
        ("fact_orders_valid_date_key", "ERROR",
         "SELECT COUNT(*) AS violations FROM gold.fact_orders f "
         "LEFT ANTI JOIN gold.dim_date d ON f.date_key = d.date_key"),
        ("daily_sales_non_negative_revenue", "ERROR",
         "SELECT COUNT(*) AS violations FROM gold.daily_sales WHERE revenue < 0"),
    ],
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in CHECKS:
        raise SystemExit(f"usage: data_quality.py <{'|'.join(CHECKS)}>")
    stage = sys.argv[1]

    spark = get_spark(f"data_quality_{stage}")
    try:
        ensure_namespaces(spark)
        spark.sql("""
            CREATE TABLE IF NOT EXISTS audit.dq_results (
                run_ts TIMESTAMP, stage STRING, check_name STRING,
                severity STRING, violations BIGINT, passed BOOLEAN
            ) USING iceberg
        """)

        failures = []
        for name, severity, sql in CHECKS[stage]:
            violations = int(spark.sql(sql).first()["violations"])
            passed = violations == 0
            marker = "PASS" if passed else severity
            print(f"[{marker:5}] {stage}.{name}: {violations} violation(s)")
            spark.sql(f"""
                INSERT INTO audit.dq_results
                VALUES (current_timestamp(), '{stage}', '{name}',
                        '{severity}', {violations}, {str(passed).lower()})
            """)
            if not passed and severity == "ERROR":
                failures.append(name)

        if failures:
            raise SystemExit(
                f"data quality FAILED for stage '{stage}': {', '.join(failures)}")
        print(f"data quality passed for stage '{stage}'")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
