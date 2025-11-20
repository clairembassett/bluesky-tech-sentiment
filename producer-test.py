import requests 
import time
import json
import logging
from quixstreams import Application

# FIRST PRODUCER, WORKS!

API_KEY = "0bf17253e36d46e3916ef825d6870e37"

# url = "https://newsapi.org/v2/top-headlines?country=us&apiKey=0bf17253e36d46e3916ef825d6870e37"
# url = "https://newsapi.org/v2/everything?q=Apple&apiKey=API_KEY"

url = "https://newsapi.org/v2/everything?q=cyber&from=2025-10-21&sortBy=publishedAt&apiKey=0bf17253e36d46e3916ef825d6870e37"

# tapping into the api 
def get_info():
    params = {
        "sortBy": "publishedAt",
        "apiKey": API_KEY 
    }
    response = requests.get(
        url, params=params 
    )
    return response.json() 

def main():
    app = Application(
        broker_address = "localhost:19092",
        loglevel="DEBUG",
    )

    with app.get_producer() as producer:
        while True: 
            info = get_info()
            logging.debug(f"Got info: {info}")
            producer.produce(
                topic="news3",
                key="news3_key",
                value=json.dumps(info),
            )
            logging.info("Produced. Sleeping...")
            time.sleep(10)


if __name__ == "__main__":
    try: 
        logging.basicConfig(level="DEBUG")
        main() 
    except KeyboardInterrupt:
        pass 

    