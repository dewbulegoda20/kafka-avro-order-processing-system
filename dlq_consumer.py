"""
DLQ viewer.

A tiny read-only consumer for the Dead Letter Queue topic, so you can show
your marker exactly what ended up there and why. Reads from the beginning
of the topic every time (own throwaway consumer group) and exits after a
short idle period -- it's a viewer, not a long-running service.

Usage:
    python dlq_consumer.py
"""
import argparse
import io
import uuid

import fastavro
from confluent_kafka import Consumer

DLQ_SCHEMA = fastavro.schema.load_schema("schemas/order_dlq.avsc")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9094")
    parser.add_argument("--dlq-topic", default="orders-dlq")
    parser.add_argument("--idle-timeout", type=float, default=5.0,
                         help="seconds of no new messages before this viewer exits")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": f"dlq-viewer-{uuid.uuid4()}",  # fresh group every run -> always reads from earliest
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([args.dlq_topic])

    print(f"[dlq-viewer] reading '{args.dlq_topic}' from the beginning...\n")
    found = 0
    try:
        while True:
            msg = consumer.poll(args.idle_timeout)
            if msg is None:
                break
            if msg.error():
                print(f"[dlq-viewer] Kafka error: {msg.error()}")
                continue
            record = fastavro.schemaless_reader(io.BytesIO(msg.value()), DLQ_SCHEMA)
            found += 1
            print(f"{found}. {record}")
    finally:
        consumer.close()

    if found == 0:
        print("(no messages in the DLQ yet)")
    else:
        print(f"\n[dlq-viewer] {found} message(s) in the DLQ.")


if __name__ == "__main__":
    main()
