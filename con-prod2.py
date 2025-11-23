from quixstreams import Application
import json
import time

def main():
    app = Application(
        broker_address="localhost:19092",
        consumer_group="gdelt_splitter_group",
        auto_offset_reset="earliest",
        loglevel="DEBUG",
    )

    consumer = app.get_consumer()
    consumer.subscribe(["gdelt_cyber"])  # from your producer

    producer = app.get_producer()

    print("GDELT Splitter running...")

    while True:
        msg = consumer.poll(1)

        if msg is None:
            print("Waiting for messages...")
            continue

        if msg.error():
            raise Exception(msg.error())

        # decode one article (already a dict)
        raw = msg.value().decode("utf-8")
        article = json.loads(raw)

        key = msg.key().decode("utf-8") if msg.key() else "no_key"

        print(f"Received article from key: {key}")

        # send to new topic
        producer.produce(
            topic="gdelt_articles",
            key=key,
            value=json.dumps(article)
        )

        producer.flush()
        print("Forwarded article.\n")

        consumer.store_offsets(msg)
        time.sleep(0.5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping splitter...") 
    except Exception as e:
        print(f"Error: {e}")
