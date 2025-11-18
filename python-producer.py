import requests
import json
from kafka import KafkaProducer
import time

# === CONFIG === 
OTX_API_KEY = "d5d6569f63ac5529eaedd477c6c86b27e167add7cc6bc4a2caf40e8de67fb3e7"
OTX_URL = "https://otx.alienvault.com/api/v1/indicators/export"  # example endpoint
KAFKA_BROKER = "localhost:8080"
KAFKA_TOPIC = "otx-stream"

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Headers for OTX API
headers = {
    "X-OTX-API-KEY": OTX_API_KEY
}

# Poll interval (seconds)
POLL_INTERVAL = 60  # fetch new data every minute

while True:
    try:
        response = requests.get(OTX_URL, headers=headers)
        if response.status_code == 200:
            data = response.json()
            # Send each item to Kafka
            for item in data.get("results", []):
                producer.send(KAFKA_TOPIC, value=item)
            print(f"Sent {len(data.get('results', []))} items to Kafka")
        else:
            print(f"Error fetching OTX data: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"Exception: {e}")
    
    time.sleep(POLL_INTERVAL)

