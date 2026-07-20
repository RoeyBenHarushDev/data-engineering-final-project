"""Inspection utility for the demo: prints row counts and sample rows per layer.

Usage:
    spark-submit show_tables.py [bronze|silver|gold|audit|all]
"""

import sys

from spark_config import get_spark

TABLES = {
    "bronze": ["bronze.customers_raw", "bronze.products_raw",
               "bronze.orders_raw", "bronze.order_events"],
    "silver": ["silver.customers", "silver.products", "silver.orders",
               "silver.order_events", "silver.order_events_rejected"],
    "gold": ["gold.dim_customer", "gold.dim_product", "gold.dim_date",
             "gold.fact_orders", "gold.daily_sales"],
    "audit": ["audit.dq_results", "audit.ge_results"],
}


def main():
    scope = sys.argv[1] if len(sys.argv) > 1 else "all"
    layers = TABLES if scope == "all" else {scope: TABLES[scope]}

    spark = get_spark("show_tables")
    try:
        for layer, tables in layers.items():
            print(f"\n{'=' * 60}\nLAYER: {layer.upper()}\n{'=' * 60}")
            for t in tables:
                try:
                    df = spark.table(t)
                    print(f"\n--- {t} ({df.count()} rows) ---")
                    df.show(5, truncate=False)
                except Exception as e:  # table may not exist yet
                    print(f"\n--- {t}: not available ({type(e).__name__}) ---")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
