# Kafka Order Processing System

A Kafka-based pipeline that produces and consumes `Order` messages, Avro-encoded,
with real-time running-average aggregation, retry logic for transient failures,
and a Dead Letter Queue (DLQ) for messages that permanently fail.

## Architecture

```
                 ┌──────────────┐        topic: orders        ┌──────────────┐
 producer.py --> │  Kafka broker │ ───────────────────────────> │ consumer.py  │
 (random orders) │ (Docker,      │                              │              │
                 │  KRaft mode)  │                              │ - decode     │
                 └──────────────┘                              │ - validate   │
                        ^                                      │ - process +  │
                        │      topic: orders-dlq                │   retry      │
                        └──────────────────────────────────────  │ - running    │
                                    (permanent failures)          │   average    │
                                                                   └──────┬───────┘
                                                                          │
                                                                dlq_consumer.py
                                                                (view DLQ contents)
```

**Why no Schema Registry?** For an assignment of this scope, a Confluent Schema
Registry adds an extra moving part (another container, another dependency)
without changing the core learning goals. Both producer and consumer read the
same `schemas/*.avsc` files directly and use `fastavro.schemaless_writer` /
`schemaless_reader` to encode/decode Avro. This is a legitimate, commonly-used
Avro pattern (just without centralized schema versioning) — worth mentioning
in your report as a deliberate design trade-off.

## Message flow / failure handling

Every message goes through, in order:

1. **Avro decode.** If the bytes aren't valid Avro at all, it's a permanent
   failure straight away → **DLQ** (`errorReason: AVRO_DECODE_ERROR`), no retries
   (retrying garbage bytes will never fix them).
2. **Business validation.** `price <= 0` is treated as invalid input → **DLQ**
   (`errorReason: invalid price...`), also no retries (a retry wouldn't change
   the price).
3. **Processing + running average.** This step simulates a downstream call
   that can fail *transiently* (`--failure-rate`, default 20% chance). On
   failure, the consumer retries up to `--max-retries` times (default 3) with
   exponential backoff (1s, 2s, 4s, ...). If it still fails after all retries
   → **DLQ** (`errorReason: MAX_RETRIES_EXCEEDED`). If it succeeds, the running
   average of `price` is updated and printed.

The DLQ is a separate Kafka topic (`orders-dlq`), not just a log line — so a
downstream team could later replay/inspect/reprocess those messages, which is
the point of a real DLQ.

## Prerequisites

- Docker + Docker Compose
- Python 3.10+

## Setup

```bash
# 1. Start Kafka (single-node, KRaft mode — no Zookeeper needed)
docker compose up -d

# 2. give it ~10-15s to finish starting, then check it's healthy:
docker compose logs -f kafka
# look for a line like "Kafka Server started" then Ctrl+C

# 3. Python environment
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Running it

Open **two terminals** (both with the venv activated):

**Terminal 1 — consumer:**
```bash
python consumer.py
```

**Terminal 2 — producer:**
```bash
python producer.py --rate 2
```

You'll see the producer sending random orders, and the consumer printing the
running average price as it processes them. Because `--failure-rate` defaults
to 0.2, you should organically see some `retrying in 1.0s` messages, and
occasionally a message land in the DLQ after exhausting retries.


 
