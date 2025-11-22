import requests
import json
import time
import logging
from quixstreams import Application

# ---------------------------------------
# GDELT DOC API endpoint
# ---------------------------------------
GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

def fetch_gdelt_data(query="cyber"):
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json"
    }

    response = requests.get(GDELT_URL, params=params, timeout=10)
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
                data = fetch_gdelt_data(query="cyber security")
                articles = data.get("articles", [])

                logging.info(f"Fetched {len(articles)} articles from GDELT")

                for article in articles:
                    if total_count >= TARGET:
                        break

                    producer.produce(
                        topic="gdelt_cyber",
                        key=article.get("url", "no_url"),
                        value=json.dumps(article),
                    )
                    total_count += 1

                logging.info(f"Total collected so far: {total_count}")

                # Stop once 1000 reached
                if total_count >= TARGET:
                    break

                # Wait a bit before the next API call
                time.sleep(2)

            except Exception as e:
                logging.error(f"Error: {e}")
                time.sleep(2)

    print("\n🎉 Finished! Collected exactly 1000 articles.\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopped.")
    except Exception as e:
        print(f"Error: {e}")
        raise