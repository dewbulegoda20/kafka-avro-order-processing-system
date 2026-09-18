"""
Order producer.

Generates random order messages, encodes them with Avro (using the local
order.avsc schema -- no Schema Registry needed), and publishes them to Kafka.

Usage:
    python producer.py                        # stream random orders forever, 1/sec
    python producer.py --rate 5 --count 50     # 50 orders at 5/sec then exit
    python producer.py --bad-price             # send ONE order with a negative price,
                                                # then continue normally
                                                # (demonstrates the "business rule" DLQ path)
    python producer.py --corrupt               # send ONE message that is NOT valid Avro,
                                                # then continue normally
                                                # (demonstrates the "can't even decode it" DLQ path)
"""
import argparse
import io
import random
import time
import uuid
from datetime import datetime

import fastavro
from confluent_kafka import Producer

SCHEMA = fastavro.schema.load_schema("schemas/order.avsc")
PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def encode_order(order: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, SCHEMA, order)
    return buf.getvalue()


def random_order() -> dict:
    return {
        "orderId": str(uuid.uuid4())[:8],
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] delivery failed: {err}")
    else:
        print(f"[producer] sent -> partition={msg.partition()} offset={msg.offset()}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9094")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--rate", type=float, default=1.0, help="messages per second")
    parser.add_argument("--count", type=int, default=None, help="number of messages (default: run forever)")
    parser.add_argument("--bad-price", action="store_true",
                         help="send one order with a negative price, then continue normally")
    parser.add_argument("--corrupt", action="store_true",
                         help="send one non-Avro (garbage) message, then continue normally")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    sleep_time = 1.0 / args.rate if args.rate > 0 else 0

    sent = 0
    try:
        while args.count is None or sent < args.count:
            if args.bad_price and sent == 0:
                order = random_order()
                order["price"] = -round(random.uniform(1.0, 50.0), 2)
                print(f"[producer] >>> sending DELIBERATELY BAD order (negative price): {order}")
                producer.produce(args.topic, key=order["orderId"], value=encode_order(order),
                                  callback=delivery_report)
            elif args.corrupt and sent == 0:
                print("[producer] >>> sending DELIBERATELY CORRUPT (non-Avro) message")
                producer.produce(args.topic, key="corrupt", value=b"this-is-not-avro-bytes!!",
                                  callback=delivery_report)
            else:
                order = random_order()
                print(f"[producer] order: {order}")
                producer.produce(args.topic, key=order["orderId"], value=encode_order(order),
                                  callback=delivery_report)

            sent += 1
            producer.poll(0)
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        print("\n[producer] stopping...")
    finally:
        producer.flush()
        print(f"[producer] done. sent {sent} message(s) at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
