import requests
import json
import time

bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
otx_api_key = os.getenv("OTX_API_KEY")
topic_name = os.getenv("TOPIC_NAME", "otx-threat-intel")

producer = Producer({'bootstrap.servers': bootstrap_servers})

OTX_URL = "https://otx.alienvault.com/api/v1/pulses/subscribed"
headers = {"X-OTX-API-KEY": otx_api_key}
# OTX API configuration
OTX_API_KEY = "YOUR_OTX_API_KEY"
OTX_URL = "https://otx.alienvault.com/api/v1/pulses/subscribed"  # subscribed pulses feed

headers = {
    "X-OTX-API-KEY": OTX_API_KEY
}

def fetch_otx_data():
    """Fetch threat intel data from OTX API"""
    response = requests.get(OTX_URL, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching OTX data: {response.status_code}")
        return None

def produce_otx_data(producer, topic="otx-threat-intel"):
    """Fetch OTX data and send to Kafka"""
    data = fetch_otx_data()
    if data:
        for pulse in data.get("results", []):
            payload = json.dumps(pulse)
            producer.produce(topic, value=payload)
        producer.flush()
        print(f"Sent {len(data.get('results', []))} pulses to Kafka.")

if __name__ == "__main__":
    while True:
        produce_otx_data(producer)  # assumes you already created `producer`
        time.sleep(300)  # fetch every 5 minutes

