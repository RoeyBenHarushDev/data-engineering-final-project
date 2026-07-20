"""Generates the initial batch datasets (CSV) used to seed the bronze layer.

Deterministic (fixed seed) so the committed files can be regenerated at any time:
    python generate_initial_data.py [output_dir]

Produces:
    customers_day1.csv  initial customer snapshot
    customers_day2.csv  later snapshot with ~8% changed rows (drives the SCD2 demo)
    products.csv        product dimension source
    orders.csv          60 days of order history (contains a few dirty rows on purpose,
                        so the data quality checks have something to catch)
"""

import csv
import os
import random
import sys
from datetime import datetime, timedelta

SEED = 42
N_CUSTOMERS = 500
N_PRODUCTS = 100
N_ORDERS = 5000
BASE_DATE = datetime(2026, 5, 1, 0, 0, 0)

FIRST = ["Noa", "Adam", "Maya", "Daniel", "Tamar", "Yoni", "Shira", "Omer", "Lia", "Eitan",
         "Roni", "Amit", "Yael", "Ido", "Gal", "Noam", "Dana", "Uri", "Michal", "Ben"]
LAST = ["Levi", "Cohen", "Mizrahi", "Peretz", "Biton", "Avraham", "Friedman", "Katz",
        "Shapiro", "Azulay", "Dahan", "Malka", "Harel", "Segal", "Baruch"]
CITIES = ["Tel Aviv", "Jerusalem", "Haifa", "Beer Sheva", "Netanya", "Ashdod", "Holon",
          "Rishon LeZion", "Petah Tikva", "Herzliya"]
SEGMENTS = ["consumer", "business", "premium"]
CATEGORIES = ["Electronics", "Home", "Sports", "Books", "Toys", "Fashion", "Grocery"]
STATUSES = ["completed", "completed", "completed", "shipped", "cancelled"]


def gen_customers(rng):
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        name = f"{rng.choice(FIRST)} {rng.choice(LAST)}"
        rows.append({
            "customer_id": i,
            "full_name": name,
            "email": f"{name.lower().replace(' ', '.')}.{i}@example.com",
            "city": rng.choice(CITIES),
            "segment": rng.choice(SEGMENTS),
            "signup_date": (BASE_DATE - timedelta(days=rng.randint(30, 720))).strftime("%Y-%m-%d"),
            "updated_at": BASE_DATE.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return rows


def gen_day2(rng, customers):
    day2 = []
    changed = rng.sample(range(len(customers)), k=int(N_CUSTOMERS * 0.08))
    for idx, c in enumerate(customers):
        c2 = dict(c)
        if idx in changed:
            # customer moved city and/or changed segment -> SCD2 must open a new version
            c2["city"] = rng.choice([x for x in CITIES if x != c["city"]])
            if rng.random() < 0.5:
                c2["segment"] = rng.choice([x for x in SEGMENTS if x != c["segment"]])
            c2["updated_at"] = (BASE_DATE + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        day2.append(c2)
    return day2


def gen_products(rng):
    rows = []
    for i in range(1, N_PRODUCTS + 1):
        cat = rng.choice(CATEGORIES)
        rows.append({
            "product_id": i,
            "product_name": f"{cat} item {i}",
            "category": cat,
            "unit_price": round(rng.uniform(5, 900), 2),
        })
    return rows


def gen_orders(rng, products):
    rows = []
    for i in range(1, N_ORDERS + 1):
        p = rng.choice(products)
        qty = rng.randint(1, 5)
        ts = BASE_DATE - timedelta(days=rng.randint(0, 60),
                                   seconds=rng.randint(0, 86399))
        rows.append({
            "order_id": f"ORD-{i:06d}",
            "customer_id": rng.randint(1, N_CUSTOMERS),
            "product_id": p["product_id"],
            "quantity": qty,
            "unit_price": p["unit_price"],
            "total_amount": round(qty * p["unit_price"], 2),
            "status": rng.choice(STATUSES),
            "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        })
    # deliberately dirty rows: silver must drop them, data quality must report them
    rows[100]["quantity"] = -3
    rows[200]["customer_id"] = ""
    rows[300]["total_amount"] = -50.0
    rows.append(dict(rows[400]))  # duplicate order_id
    return rows


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows):>5} rows -> {path}")


def main():
    out_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "data")
    os.makedirs(out_dir, exist_ok=True)
    rng = random.Random(SEED)

    customers = gen_customers(rng)
    write_csv(os.path.join(out_dir, "customers_day1.csv"), customers)
    write_csv(os.path.join(out_dir, "customers_day2.csv"), gen_day2(rng, customers))
    products = gen_products(rng)
    write_csv(os.path.join(out_dir, "products.csv"), products)
    write_csv(os.path.join(out_dir, "orders.csv"), gen_orders(rng, products))


if __name__ == "__main__":
    main()
