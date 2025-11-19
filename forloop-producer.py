import requests
import time
import json
import logging
from quixstreams import Application

API_KEY = "0bf17253e36d46e3916ef825d6870e37"

def get_info(country):
    url = "https://newsapi.org/v2/top-headlines"
    params = {
        "sortBy": "publishedAt",
        "apiKey": API_KEY,
        "country": country
    }
    response = requests.get(url, params=params)
    return response.json()

def main():
    countries = [
        'us', 'cn', 'jp', 'de', 'in', 'gb', 'fr', 'it', 'ca', 'kr',
        'ru', 'br', 'au', 'es', 'mx', 'id', 'nl', 'sa', 'sg', 'hk',
        'my', 'za', 'dk', 'ph', 'eg', 'ie', 'vn', 'cz', 'pk', 'fi',
        'ae', 'co', 'ro', 'bd', 'hu', 'cl', 'nz', 'ua', 'pt', 'gr',
        'qa', 'ng', 'lt', 'tn', 'om', 'kz', 'dz', 'lv', 'sk', 'ee',
        'bg', 'hr', 'uy', 'si', 'ec', 'ba', 'kh', 'jo', 'lb', 'mk',
        'mt', 'rs', 'ug'
    ]

    app = Application(
        broker_address="localhost:29092",
        loglevel="DEBUG",
    )

    with app.get_producer() as producer:
        while True:
            for country in countries:
                info = get_info(country)
                logging.debug(f"Got info for {country}: {info}")
                producer.produce(
                    topic="news",
                    key=f"news_key_{country}",
                    value=json.dumps(info),
                )
            logging.info("Produced all countries. Sleeping...")
            time.sleep(10)

if __name__ == "__main__":
    try: 
        logging.basicConfig(level="DEBUG")
        main() 
    except KeyboardInterrupt:
        pass
