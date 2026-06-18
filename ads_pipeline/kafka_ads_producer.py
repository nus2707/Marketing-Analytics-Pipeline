"""
Streams synthetic ad events into the Kafka topic 'ad-impressions'.
Works identically against Azure Event Hubs (same Kafka protocol surface).

Usage:
    python -m ads_pipeline.kafka_ads_producer
    python -m ads_pipeline.kafka_ads_producer --batch   # 100-msg micro-batches
"""
from __future__ import annotations
import argparse
import json
import logging
import time

from confluent_kafka import Producer
from ads_pipeline.data_generator import generate_events

TOPIC = "ad-impressions"
BOOTSTRAP_SERVERS = "localhost:9092"

logger = logging.getLogger(__name__)


def _delivery_report(err, msg):
    if err:
        logger.error("Delivery failed [%s]: %s", msg.key(), err)
    else:
        logger.debug("→ %s [partition %d] offset %d",
                     msg.topic(), msg.partition(), msg.offset())


def _build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "compression.type": "lz4",
        "linger.ms": 20,
        "batch.size": 65536,
    })


def produce_events(batch_mode: bool = False) -> None:
    producer = _build_producer()
    events = list(generate_events())
    logger.info("Producing %d events → topic '%s'", len(events), TOPIC)

    for i, event in enumerate(events):
        key = f"{event['campaign_id']}#{event['date']}"
        producer.produce(
            topic=TOPIC,
            key=key.encode(),
            value=json.dumps(event).encode(),
            on_delivery=_delivery_report,
        )
        producer.poll(0)

        if batch_mode and (i + 1) % 100 == 0:
            producer.flush()
            logger.info("Flushed batch %d", (i + 1) // 100)
            time.sleep(0.05)

    producer.flush()
    logger.info("All %d events produced.", len(events))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", action="store_true")
    args = parser.parse_args()
    produce_events(batch_mode=args.batch)
