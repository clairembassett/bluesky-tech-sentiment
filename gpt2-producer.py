#!/usr/bin/env python

import requests
import json
import time
from kafka import KafkaProducer

# === CONFIG ===
API_KEY = "d5d6569f63ac5529eaedd477c6c86b27e167add7cc6bc4a2caf40e8de67fb3e7"
OTX_URL = "https://otx.alienvault.com/api/v1/pulses/subscribed"
KAFKA_BROKER = "localhost:29092"  # Redpanda port
KAFKA_TOPIC = "otx-stream"

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

headers = {"X-OTX-API-KEY": API_KEY}
POLL_INTERVAL = 60  # fetch new data every minute

while True:
    try:
        response = requests.get(OTX_URL, headers=headers)
        if response.status_code == 200:
            data = response.json()
            pulses = data.get("results", [])
            
            for pulse in pulses:
                producer.send(KAFKA_TOPIC, pulse)
            
            producer.flush()
            print(f"Sent {len(pulses)} messages to topic '{KAFKA_TOPIC}'")
        else:
            print(f"Error fetching OTX data: {response.status_code}")
    except Exception as e:
        print(f"Exception: {e}")

    time.sleep(POLL_INTERVAL)

