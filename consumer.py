"""
Order consumer.

Reads order messages from Kafka, Avro-decodes them, and:
  1. Maintains a running average of `price` across all successfully processed orders.
  2. Retries transient processing failures (simulated) with exponential backoff.
  3. Routes permanently-failed messages to a Dead Letter Queue (DLQ) topic instead
     of blocking the pipeline or silently dropping them. Two kinds of permanent
     failure are handled:
       - the message isn't valid Avro at all (can't even be decoded)
       - the message fails a business rule (price <= 0)
       - a transient failure that never recovered after --max-retries attempts

Usage:
    python consumer.py
    python consumer.py --failure-rate 0.3 --max-retries 3
"""
import argparse
import io
import random
import time
from datetime import datetime, timezone

import fastavro
from confluent_kafka import Consumer, KafkaError, Producer

ORDER_SCHEMA = fastavro.schema.load_schema("schemas/order.avsc")
DLQ_SCHEMA = fastavro.schema.load_schema("schemas/order_dlq.avsc")


class TransientProcessingError(Exception):
    """Simulates a temporary downstream failure (e.g. a flaky service call)."""


class PermanentValidationError(Exception):
    """A business-rule violation that will never succeed no matter how many times we retry."""


# --- running average state -------------------------------------------------
running_count = 0
running_total = 0.0


def update_running_average(price: float) -> float:
    global running_count, running_total
    running_count += 1
    running_total += price
    return running_total / running_count


def validate_order(order: dict) -> None:
    if order["price"] <= 0:
        raise PermanentValidationError(f"invalid price: {order['price']} (must be > 0)")


def process_order(order: dict, failure_rate: float) -> None:
    """Simulates real processing work (e.g. writing to a DB / calling an API)
    that can fail transiently. Raises TransientProcessingError to simulate that."""
    if random.random() < failure_rate:
        raise TransientProcessingError("simulated transient downstream failure")
    avg = update_running_average(order["price"])
    print(f"[consumer] processed {order} | running average price = {avg:.2f} "
          f"(n={running_count})")


def encode_dlq_record(dlq_record: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, DLQ_SCHEMA, dlq_record)
    return buf.getvalue()


def make_dlq_record(order: dict | None, raw: bytes | None, reason: str, retry_count: int) -> dict:
    return {
        "orderId": order["orderId"] if order else None,
        "product": order["product"] if order else None,
        "price": order["price"] if order else None,
        "rawPayload": raw if order is None else None,
        "errorReason": reason,
        "retryCount": retry_count,
        "failedAtIso": datetime.now(timezone.utc).isoformat(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9094")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--dlq-topic", default="orders-dlq")
    parser.add_argument("--group-id", default="order-consumer-group")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--backoff-base", type=float, default=1.0,
                         help="seconds; backoff doubles each retry (1, 2, 4, ...)")
    parser.add_argument("--failure-rate", type=float, default=0.2,
                         help="probability [0-1] that processing a valid order fails transiently")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,   # we commit manually, only after success or DLQ routing
    })
    dlq_producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    def send_to_dlq(order, raw, reason, retry_count):
        record = make_dlq_record(order, raw, reason, retry_count)
        dlq_producer.produce(args.dlq_topic, value=encode_dlq_record(record))
        dlq_producer.flush()
        print(f"[consumer] !! sent to DLQ ({reason}): {record}")

    consumer.subscribe([args.topic])
    print(f"[consumer] listening on '{args.topic}' "
          f"(max_retries={args.max_retries}, simulated failure_rate={args.failure_rate})")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[consumer] Kafka error: {msg.error()}")
                continue

            raw = msg.value()

            # --- Step 1: try to Avro-decode the message ---
            try:
                order = fastavro.schemaless_reader(io.BytesIO(raw), ORDER_SCHEMA)
            except Exception as e:
                send_to_dlq(order=None, raw=raw, reason=f"AVRO_DECODE_ERROR: {e}", retry_count=0)
                consumer.commit(msg)
                continue

            # --- Step 2: validate business rules (never worth retrying) ---
            try:
                validate_order(order)
            except PermanentValidationError as e:
                send_to_dlq(order=order, raw=None, reason=str(e), retry_count=0)
                consumer.commit(msg)
                continue

            # --- Step 3: process with retry + exponential backoff ---
            succeeded = False
            for attempt in range(1, args.max_retries + 1):
                try:
                    process_order(order, args.failure_rate)
                    succeeded = True
                    break
                except TransientProcessingError as e:
                    backoff = args.backoff_base * (2 ** (attempt - 1))
                    print(f"[consumer] attempt {attempt}/{args.max_retries} failed ({e}); "
                          f"retrying in {backoff:.1f}s")
                    time.sleep(backoff)

            if not succeeded:
                send_to_dlq(order=order, raw=None,
                            reason=f"MAX_RETRIES_EXCEEDED after {args.max_retries} attempts",
                            retry_count=args.max_retries)

            consumer.commit(msg)

    except KeyboardInterrupt:
        print("\n[consumer] stopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
