from quixstreams import Application
import json
import time

# FIRST CONSUMER THAT UNPACKAGES MESSAGES AND SENDS EACH INDIVIDUAL ARTICLE TO A SECOND TOPIC AS A PRODUCER

def main():
    # First app: consumes and also produces new messages
    app = Application(
        broker_address="localhost:19092",
        consumer_group="news_splitter_group3",
        auto_offset_reset="earliest",
        loglevel="DEBUG",
        processing_guarantee="exactly-once"
    )

    # consumer reads from your original topic
    consumer = app.get_consumer()
    consumer.subscribe(["news3"])

    # producer sends split articles to the new topic
    producer = app.get_producer()

    print("Splitter running...")

    while True:
        msg = consumer.poll(1)

        if msg is None:
            print("Waiting for messages...")
            continue
        
        if msg.error():
            raise Exception(msg.error())

        # decode Kafka message
        value = json.loads(msg.value())
        key = msg.key().decode("utf8") if msg.key() else None
        offset = msg.offset()

        print(f"KEY: {key}")

        # Expecting {"articles": [ ... ]}
        articles = value.get("articles", [])

        for article in articles:
            # Clean or modify article if needed
            article_json = json.dumps(article)

            # Send each article as NEW message
            producer.produce(
                topic="news_articles3",     # new topic
                key="article",
                value=article_json
            )
            print("Sent ->")


        # commit offsets
        consumer.store_offsets(msg)

        time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping splitter...")
