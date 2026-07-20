"""Kafka producer simulating real-time order events for topic `orders_events`.

Each order goes through a small lifecycle (created -> paid -> shipped -> completed,
or cancelled). To exercise the pipeline's late-arrival handling, a configurable
share of events is produced with an event_time in the past:
  * LATE_EVENT_RATIO       events 1-47h old  -> must still be accepted (<= 48h rule)
  * VERY_LATE_EVENT_RATIO  events 49-72h old -> must be quarantined  (>  48h rule)
"""

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "orders_events")
EVENTS_PER_MINUTE = int(os.environ.get("EVENTS_PER_MINUTE", "60"))
LATE_RATIO = float(os.environ.get("LATE_EVENT_RATIO", "0.15"))
VERY_LATE_RATIO = float(os.environ.get("VERY_LATE_EVENT_RATIO", "0.05"))

N_CUSTOMERS = 500   # matches the batch customer dataset
N_PRODUCTS = 100    # matches the batch product dataset
LIFECYCLES = [
    ["created", "paid", "shipped", "completed"],
    ["created", "paid", "shipped", "completed"],
    ["created", "paid", "cancelled"],
    ["created", "cancelled"],
]

_running = True


def _stop(signum, frame):
    global _running
    _running = False
    log.info("shutdown signal received")


def connect() -> KafkaProducer:
    """Retries until the broker is reachable (producer may start before Kafka)."""
    while _running:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=100,
            )
            log.info("connected to kafka at %s", BOOTSTRAP)
            return producer
        except NoBrokersAvailable:
            log.warning("kafka not reachable at %s, retrying in 5s", BOOTSTRAP)
            time.sleep(5)
    sys.exit(0)


def pick_event_time(now: datetime) -> datetime:
    """Most events are fresh; some simulate late and very-late arrivals."""
    roll = random.random()
    if roll < VERY_LATE_RATIO:
        return now - timedelta(hours=random.uniform(49, 72))   # beyond the 48h rule
    if roll < VERY_LATE_RATIO + LATE_RATIO:
        return now - timedelta(hours=random.uniform(1, 47))    # late but acceptable
    return now - timedelta(seconds=random.uniform(0, 60))


def make_order_events(seq: int) -> list:
    """Builds the full lifecycle of one synthetic order."""
    now = datetime.now(timezone.utc)
    base_time = pick_event_time(now)
    order_id = f"ORD-S{seq:06d}"
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(5, 900), 2)
    customer_id = random.randint(1, N_CUSTOMERS)
    product_id = random.randint(1, N_PRODUCTS)

    events = []
    for step, status in enumerate(random.choice(LIFECYCLES)):
        events.append({
            "event_id": str(uuid.uuid4()),
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": unit_price,
            "status": status,
            "event_time": (base_time + timedelta(minutes=step * random.randint(1, 10))
                           ).strftime("%Y-%m-%d %H:%M:%S"),
            "produced_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return events


def main():
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    producer = connect()
    delay = 60.0 / max(EVENTS_PER_MINUTE, 1)
    seq = int(time.time())  # unique-ish order sequence across restarts
    sent = 0

    log.info("producing to '%s' (~%d events/min, late=%.0f%%, very_late=%.0f%%)",
             TOPIC, EVENTS_PER_MINUTE, LATE_RATIO * 100, VERY_LATE_RATIO * 100)
    while _running:
        seq += 1
        try:
            for event in make_order_events(seq):
                producer.send(TOPIC, key=event["order_id"], value=event)
                sent += 1
                if sent % 50 == 0:
                    log.info("%d events sent (last: %s %s at %s)", sent,
                             event["order_id"], event["status"], event["event_time"])
                time.sleep(delay)
            producer.flush()
        except Exception:
            log.exception("failed to send, reconnecting in 5s")
            time.sleep(5)
            producer = connect()

    producer.flush()
    producer.close()
    log.info("stopped after %d events", sent)


if __name__ == "__main__":
    main()
