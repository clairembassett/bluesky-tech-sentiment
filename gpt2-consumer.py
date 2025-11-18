#!/usr/bin/env python

from kafka import KafkaConsumer
import json

# === CONFIG ===
KAFKA_BROKER = "localhost:29092"
KAFKA_TOPIC = "otx-stream"
CONSUMER_GROUP = "otx-consumer-group"

# Create Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    group_id=CONSUMER_GROUP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print(f"Listening to topic '{KAFKA_TOPIC}'...")

for message in consumer:
    print(f"Received message: {message.value}")

