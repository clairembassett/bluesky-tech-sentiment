import requests
import json
import time
from kafka import KafkaProducer

# === CONFIG ===
API_KEY = "d5d6569f63ac5529eaedd477c6c86b27e167add7cc6bc4a2caf40e8de67fb3e7"
OTX_URL = "https://otx.alienvault.com/api/v1/pulses/subscribed"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "Cyber"
TARGET_MESSAGES = 1000       # target messages per interval
POLL_INTERVAL = 60           # seconds

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

headers = {"X-OTX-API-KEY": API_KEY}

while True:
    try:
        # Fetch data from OTX
        response = requests.get(OTX_URL, headers=headers)
        if response.status_code == 200:
            data = response.json()
            pulses = data.get("results", [])
            num_pulses = len(pulses)
            messages_sent = 0

            if num_pulses == 0:
                print("No new pulses from OTX")
            else:
                # Keep sending until we reach TARGET_MESSAGES
                while messages_sent < TARGET_MESSAGES:
                    for pulse in pulses:
                        producer.send(KAFKA_TOPIC, value=pulse)
                        messages_sent += 1
                        if messages_sent >= TARGET_MESSAGES:
                            break

                producer.flush()
                print(f"Sent {messages_sent} messages to Kafka in the last {POLL_INTERVAL} seconds")
        else:
            print(f"Error fetching OTX data: {response.status_code}")

    except Exception as e:
        print(f"Exception: {e}")

    # Sleep until next poll
    time.sleep(POLL_INTERVAL)

