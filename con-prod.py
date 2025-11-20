from quixstreams import Application
import json
import time

# FIRST CONSUMER THAT UNPACKAGES MESSAGES AND SENDS EACH INDIVIDUAL ARTICLE TO A SECOND TOPIC AS A PRODUCER

def main():
    # consumer app, that makes new consumer group and processing everything exactly once
    app = Application(
        broker_address="localhost:19092",
        consumer_group="news_splitter_group4",
        auto_offset_reset="earliest",
        loglevel="DEBUG",
        # using default of at-least-once 
        # processing_guarantee="exactly-once"
    )

    # consumer reads from original topic
    consumer = app.get_consumer()
    consumer.subscribe(["news4"])

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
        value = json.loads(msg.value().decode("utf-8")) 
        key = msg.key().decode("utf8") if msg.key() else None
        offset = msg.offset()

        print(f"Received key: {key}")

        # Expecting {"articles": [ ... ]}
        articles = value.get("articles", [])

        for i, article in enumerate(articles):
            # Clean or modify article if needed
            article_json = json.dumps(article).encode("utf-8")

            # Send each article as NEW message
            producer.produce(
                # create second new topic for individual articles
                topic="news_articles4",  
                # giving unique key based on article # 
                key=f"article-{i}",
                value=article_json
            )
            print("Sent ->")

        # ensures all produced messages are delivered to kafka before commiting offset
        producer.flush() 
        print(f"successfull sent {len(articles)} articles.")

        # now commit offsets after successful production and flush 
        consumer.store_offsets(msg)

        # sleeping 
        time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping splitter...")
    except Exception as e:
        print(f"Error: {e}")
