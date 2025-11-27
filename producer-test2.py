import requests
import json
import time
import logging
from quixstreams import Application

# ---------------------------------------
# GDELT DOC API endpoint
# ---------------------------------------
# GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

TOPIC_NAME = "gdeltUS"

def fetch_gdelt_data(query="sourcecountry:US"):
#def fetch_gdelt_data():
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json"
    }

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    while True:
        response = requests.get(GDELT_URL, params=params, headers=headers, timeout=10)
        
        # handle rate limits from api 
        if response.status_code == 429:
            logging.warning("429 Too Many Requests – backing off for 2 minutes...")
            time.sleep(120)    
            continue 
        response.raise_for_status()
        return response.json()


# ---------------------------------------
# Kafka Producer
# ---------------------------------------
def main():
    logging.basicConfig(level=logging.INFO)

    TARGET = 1000
    
    total_count = 0

    app = Application(
        broker_address="localhost:19092",
        loglevel="DEBUG",
    )

    with app.get_producer() as producer:
        while total_count < TARGET:
            try:
                # data = fetch_gdelt_data(query="cyber")
                data = fetch_gdelt_data() 
                articles = data.get("articles", [])

                logging.info(f"Fetched {len(articles)} articles from GDELT")

                for article in articles:
                    if total_count >= TARGET:
                        break

                    producer.produce(
                        topic=TOPIC_NAME,
                        key=article.get("url", "no_url"),
                        value=json.dumps(article),
                    )
                    total_count += 1

                logging.info(f"Total collected so far: {total_count}")

                # Stop once 1000 reached
                if total_count >= TARGET:
                    break

                # Wait a bit before the next API call
                time.sleep(10)

            except Exception as e:
                logging.error(f"Error: {e}")
                time.sleep(10)

    print("\n Finished!\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopped.")
    except Exception as e:
        print(f"Error: {e}")
        raise