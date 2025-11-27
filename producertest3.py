import requests
import json
import time
import logging
from quixstreams import Application

# ---------------------------------------
# GDELT DOC API endpoint
# ---------------------------------------
# GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc?maxrecords=175"

def fetch_gdelt_data(query="sourcecountry:US"):
    """Fetch recent GDELT articles, handle rate limits and empty responses."""
    params = {
        "query": "query",
        "mode": "ArtList",
        "format": "json",
        "timespan": "14d",       # last 14 days
        "sort": "DateDesc"       # newest first
    }

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    while True:
        try:
            response = requests.get(GDELT_URL, params=params, headers=headers, timeout=10)

            # handle rate limits
            if response.status_code == 429:
                logging.warning("429 Too Many Requests – waiting 2 minutes...")
                time.sleep(120)
                continue

            response.raise_for_status()

            # skip empty responses
            if not response.text.strip():
                logging.warning("Empty response from GDELT, retrying in 5s...")
                time.sleep(5)
                continue

            return response.json()

        except json.JSONDecodeError as e:
            logging.warning(f"Invalid JSON received: {e}, retrying in 5s...")
            time.sleep(5)
        except requests.RequestException as e:
            logging.error(f"Request failed: {e}, retrying in 10s...")
            time.sleep(10)


# ---------------------------------------
# Kafka Producer
# ---------------------------------------
def main():
    logging.basicConfig(level=logging.INFO)

    TARGET = 250 
    total_sent = 0
    seen_urls = set()  # deduplicate by URL

    app = Application(
        broker_address="localhost:19092",
        loglevel="DEBUG",
    )

    with app.get_producer() as producer:
        while total_sent < TARGET:
            data = fetch_gdelt_data()
            articles = data.get("articles", [])

            if not articles:
                logging.warning("No articles returned, retrying...")
                time.sleep(5)
                continue

            logging.info(f"Fetched {len(articles)} articles")

            for article in articles:
                url = article.get("url")
                if not url or url in seen_urls:
                    continue

                seen_urls.add(url)

                # send to Kafka
                producer.produce(
                    topic="gdelt6",
                    key=url,
                    value=json.dumps(article)
                )

                total_sent += 1
                logging.info(f"Sent {total_sent} / {TARGET}")

                if total_sent >= TARGET:
                    break

            # small sleep to avoid hammering API
            time.sleep(5)

    logging.info(f"🎉 Finished! Sent {total_sent} unique articles to Kafka.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopped by user.")
