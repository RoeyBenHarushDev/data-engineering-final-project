# Data Quality

Two layers of checks, both scheduled by the `batch_etl` DAG and both writing an
audit trail into Iceberg:

1. **Native checks** — `processing/jobs/data_quality.py`, run after each stage
   (`dq_bronze`, `dq_silver`, `dq_gold` tasks). ERROR-level failures abort the DAG
   run and block downstream stages; WARN-level findings are recorded only (bronze
   is raw by design, so dirty rows there are expected and reported, not fatal).
2. **Great Expectations (bonus)** — `processing/jobs/ge_quality.py`, a declarative
   expectation suite over silver and gold, run as the final DAG task.

Results: `audit.dq_results` and `audit.ge_results` (query them from spark-sql or
`show_tables.py audit`).

## Native checks per stage

### bronze (WARN unless noted — raw layer reports, doesn't block)
| Check | Severity |
|---|---|
| orders_raw / customers_raw / products_raw not empty | ERROR |
| orders_raw null business keys | WARN |
| orders_raw duplicate order_id | WARN |
| orders_raw negative quantity / amount | WARN |
| order_events null event_id | WARN |

### silver (all ERROR — the contract layer)
| Check |
|---|
| orders not empty |
| order_id unique |
| no null order/customer keys |
| quantity strictly positive |
| total_amount non-negative |
| every order references an existing customer |
| event_id unique in order_events |
| no accepted event older than the 48h lateness limit |

### gold (all ERROR)
| Check |
|---|
| fact_orders not empty |
| fact_orders order_id unique |
| dim_customer has exactly one current version per customer (SCD2 integrity) |
| every fact row joins to a dim_customer surrogate key |
| every fact row joins to dim_date |
| daily_sales revenue non-negative |

## Great Expectations suite (bonus)

- `silver.orders`: not-null / unique `order_id`, not-null `customer_id`,
  `quantity` in [1, 1000], `total_amount` >= 0, `status` in the allowed set,
  row count >= 1
- `silver.order_events`: not-null / unique `event_id`, `lateness_hours` <= 48
- `gold.fact_orders`: not-null / unique `order_id`, not-null `customer_sk`,
  `total_amount` >= 0, row count >= 1

## Demonstrating the checks

The generated `orders.csv` deliberately contains a negative quantity, a missing
customer_id, a negative amount and a duplicated order_id. Expected behavior:
bronze DQ reports them as WARN, the silver transformation filters them out, and
silver/gold DQ pass clean. The producer likewise emits ~5% events older than 48h,
which must appear in `silver.order_events_rejected`, never in silver/gold facts.
