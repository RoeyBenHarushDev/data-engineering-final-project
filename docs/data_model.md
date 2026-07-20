# Data Model

Domain: e-commerce orders. Two sources feed the lake — a batch order history
(CSV snapshots) and a real-time order-event stream (Kafka). The model follows the
medallion architecture: bronze keeps raw data, silver keeps cleaned and conformed
entities, gold is a star schema for analytics.

## Bronze — raw ingested data

```mermaid
erDiagram
    customers_raw {
        int customer_id
        string full_name
        string email
        string city
        string segment
        date signup_date
        timestamp updated_at
        string snapshot
        timestamp _ingested_at
    }
    products_raw {
        int product_id
        string product_name
        string category
        double unit_price
        timestamp _ingested_at
    }
    orders_raw {
        string order_id
        int customer_id
        int product_id
        int quantity
        double unit_price
        double total_amount
        string status
        timestamp order_ts
        timestamp _ingested_at
    }
    order_events {
        string event_id
        string order_id
        int customer_id
        int product_id
        int quantity
        double unit_price
        string status
        timestamp event_time
        timestamp produced_at
        timestamp _ingested_at
        int kafka_partition
        bigint kafka_offset
    }
```

Bronze is append-only and unvalidated: dirty rows (nulls, duplicates, negative
amounts) stay here on purpose and are reported by the bronze DQ checks.
`order_events` is partitioned by `days(event_time)`.

## Silver — cleaned and conformed

```mermaid
erDiagram
    customers ||--o{ orders : places
    products ||--o{ orders : contains
    orders ||--o{ order_events : "derived from"

    customers {
        int customer_id PK
        string full_name
        string email
        string city
        string segment
        date signup_date
        timestamp updated_at
    }
    products {
        int product_id PK
        string product_name
        string category
        double unit_price
    }
    orders {
        string order_id PK
        int customer_id FK
        int product_id FK
        int quantity
        double unit_price
        double total_amount
        string status
        timestamp order_ts
        string source "batch | stream"
        timestamp updated_at
    }
    order_events {
        string event_id PK
        string order_id
        int customer_id
        int product_id
        int quantity
        double unit_price
        string status
        timestamp event_time
        timestamp produced_at
        timestamp _ingested_at
        double lateness_hours
    }
    order_events_rejected {
        string event_id PK
        double lateness_hours
        string reject_reason
        timestamp rejected_at
    }
```

Silver rules:
- deduplicated on business keys (`order_id`, `event_id`, `customer_id`, `product_id`)
- invalid rows removed (null keys, `quantity <= 0`, `total_amount < 0`)
- **late-arrival rule**: events ingested more than 48h after `event_time` go to
  `order_events_rejected` with a reason; events up to 48h late are merged into
  `orders` out of order (MERGE by `order_id`, latest `event_time` wins)

## Gold — star schema

```mermaid
erDiagram
    dim_customer ||--o{ fact_orders : customer_sk
    dim_product ||--o{ fact_orders : product_sk
    dim_date ||--o{ fact_orders : date_key
    fact_orders ||--o{ daily_sales : "aggregated into"

    dim_customer {
        bigint customer_sk PK "surrogate key"
        int customer_id "business key"
        string full_name
        string email
        string city "SCD2 tracked"
        string segment "SCD2 tracked"
        date signup_date
        timestamp effective_from
        timestamp effective_to "null while current"
        boolean is_current
    }
    dim_product {
        int product_sk PK
        int product_id
        string product_name
        string category
        double unit_price
    }
    dim_date {
        int date_key PK "yyyyMMdd"
        date full_date
        int year
        int quarter
        int month
        int day
        string day_name
        boolean is_weekend
    }
    fact_orders {
        string order_id PK
        int date_key FK
        bigint customer_sk FK
        int product_sk FK
        int quantity
        double unit_price
        double total_amount
        string status
        timestamp order_ts
        string source
    }
    daily_sales {
        int date_key FK
        string category
        bigint orders_count
        bigint units_sold
        double revenue
    }
```

Design notes:
- `dim_customer` is a **Type 2 SCD**: a change in any tracked attribute closes the
  current row (`is_current = false`, `effective_to` set) and opens a new version with
  a new surrogate key. Facts join on the version whose validity window covers the
  order timestamp, so historical orders keep pointing at the historical attributes.
- `dim_product` is Type 1 (overwrite); its surrogate key equals the business key,
  which is acceptable for a no-history dimension and documented here explicitly.
- `fact_orders` grain: one row per order. `daily_sales` grain: date x category,
  completed/shipped orders only.

## Audit

`audit.dq_results` (native checks) and `audit.ge_results` (Great Expectations)
record every validation run: timestamp, stage/table, check name, violations, passed.
