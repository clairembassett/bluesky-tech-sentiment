import json
import logging
from quixstreams import Application

def main():
    # Configure logging with timestamps and levels
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    app = Application(
        broker_address="localhost:29092",
        loglevel="DEBUG",
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["news"])
        logging.info("Consumer subscribed to topic 'news'")

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            try:
                # Call the methods, not attributes
                key_bytes = msg.key()
                value_bytes = msg.value()

                key = key_bytes.decode("utf-8") if key_bytes else None
                value = value_bytes.decode("utf-8")

                data = json.loads(value)

                logging.info(
                    "\n--- New Message ---\n"
                    f"Topic: {msg.topic}\n"
                    f"Key:   {key}\n"
                    f"Value: {json.dumps(data, indent=2)}\n"
                    "-------------------"
                )
            except Exception as e:
                logging.error(f"Error processing message: {e}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Consumer stopped by user")
