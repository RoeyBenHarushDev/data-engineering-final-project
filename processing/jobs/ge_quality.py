"""Bonus: Great Expectations validation suite over the silver and gold layers.

Complements data_quality.py (the required native checks) with declarative
expectation suites. Results are stored in audit.ge_results; any failed
expectation fails the job and therefore the Airflow task.
"""

import json

from great_expectations.dataset import SparkDFDataset

from spark_config import get_spark, ensure_namespaces


def validate(spark, table: str, build_expectations) -> list:
    """Runs an expectation suite against an Iceberg table, returns result rows."""
    ds = SparkDFDataset(spark.table(table))
    build_expectations(ds)
    result = ds.validate()
    rows = []
    for r in result["results"]:
        cfg = r["expectation_config"]
        rows.append((
            table,
            cfg["expectation_type"],
            json.dumps({k: v for k, v in cfg["kwargs"].items()
                        if k != "result_format"}),
            bool(r["success"]),
        ))
        marker = "PASS" if r["success"] else "FAIL"
        print(f"[{marker}] {table}: {cfg['expectation_type']} {cfg['kwargs']}")
    return rows


def silver_orders_suite(ds):
    ds.expect_column_values_to_not_be_null("order_id")
    ds.expect_column_values_to_be_unique("order_id")
    ds.expect_column_values_to_not_be_null("customer_id")
    ds.expect_column_values_to_be_between("quantity", min_value=1, max_value=1000)
    ds.expect_column_values_to_be_between("total_amount", min_value=0)
    ds.expect_column_values_to_be_in_set(
        "status", ["created", "paid", "shipped", "completed", "cancelled"])
    ds.expect_table_row_count_to_be_between(min_value=1)


def silver_order_events_suite(ds):
    ds.expect_column_values_to_not_be_null("event_id")
    ds.expect_column_values_to_be_unique("event_id")
    ds.expect_column_values_to_be_between("lateness_hours", max_value=48)


def gold_fact_orders_suite(ds):
    ds.expect_column_values_to_not_be_null("order_id")
    ds.expect_column_values_to_be_unique("order_id")
    ds.expect_column_values_to_not_be_null("customer_sk")
    ds.expect_column_values_to_be_between("total_amount", min_value=0)
    ds.expect_table_row_count_to_be_between(min_value=1)


def main():
    spark = get_spark("great_expectations_quality")
    try:
        ensure_namespaces(spark)
        spark.sql("""
            CREATE TABLE IF NOT EXISTS audit.ge_results (
                run_ts TIMESTAMP, table_name STRING, expectation STRING,
                kwargs STRING, success BOOLEAN
            ) USING iceberg
        """)

        all_rows = []
        all_rows += validate(spark, "silver.orders", silver_orders_suite)
        all_rows += validate(spark, "silver.order_events", silver_order_events_suite)
        all_rows += validate(spark, "gold.fact_orders", gold_fact_orders_suite)

        for table, expectation, kwargs, success in all_rows:
            kwargs_sql = kwargs.replace("'", "''")
            spark.sql(f"""
                INSERT INTO audit.ge_results
                VALUES (current_timestamp(), '{table}', '{expectation}',
                        '{kwargs_sql}', {str(success).lower()})
            """)

        failed = [r for r in all_rows if not r[3]]
        if failed:
            raise SystemExit(
                f"Great Expectations FAILED: {len(failed)} expectation(s) not met")
        print(f"Great Expectations passed: {len(all_rows)} expectations met")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
