from quixstreams import Application
import json
import time

def main():

    Target = 1000
    total_count = 0

    app = Application(
        broker_address="localhost:19092",
        consumer_group="gdelt_splitter_group",
        auto_offset_reset="earliest",
        loglevel="DEBUG",
    )

    producer = app.get_producer()

    with app.get_consumer() as consumer:
        consumer.subscribe(["gdelt_cyber"])

        print("GDELT Splitter running...")

        while total_count < Target:

            msg = consumer.poll(1)

            if msg is None:
                print("Waiting for messages...")
                continue

            if msg.error():
                raise Exception(msg.error())

            # decode one article (json string → dict)
            raw = msg.value().decode("utf-8")
            article = json.loads(raw)

            key = msg.key().decode("utf-8") if msg.key() else "no_key"

            print(f"Received article from key: {key}")

            # forward to new topic
            producer.produce(
                topic="gdelt_articles",
                key=key,
                value=json.dumps(article)
            )

            producer.flush()

            total_count += 1
            print(f"Forwarded article #{total_count}/{Target}\n")

            consumer.store_offsets(msg)
            time.sleep(0.5)

    print("Target reached — splitter stopping.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping splitter...") 
    except Exception as e:
        print(f"Error: {e}")
