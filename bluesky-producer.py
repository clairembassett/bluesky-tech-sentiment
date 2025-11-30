import asyncio
import websockets
from quixstreams import Application
import os
import json
import logging


# Set up logging
logging.basicConfig(level=logging.INFO)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
uri = "wss://jetstream2.us-west.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
TOPIC_NAME = "bluesky6"

# Async allows python to buffer if needed 
async def listen_to_bluesky():

    app = Application(
        broker_address=KAFKA_BROKER,
        # consumer_group="wikipedia-producer",
        producer_extra_config={
            # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
            "broker.address.family": "v4",
        }
    )   
    topic = app.topic(
        name=TOPIC_NAME,
        value_serializer="json",
    )

    message_count = 0

    while True:
      try:
        async with websockets.connect(
            uri,
            ping_interval=20,  # Send ping every 20 seconds
            ping_timeout=60,   # Wait 60 seconds for pong response
            close_timeout=10
        ) as websocket:
          logging.info("Connected to Bluesky firehose...")
          while True:
            try:
              message = await websocket.recv()
              data = json.loads(message)

             
              # Double checks post
              if data.get("commit", {}).get("record", {}).get("$type") != "app.bsky.feed.post":
                  continue
              
              rev = data.get("commit", {}).get("rev", "unknown")
                    
              # Serialize the event
              serialized = topic.serialize(key=rev, value=data)
              # Produce to Kafka
              with app.get_producer() as producer:
                  producer.produce(
                      topic=TOPIC_NAME,
                      key=serialized.key,   
                      value=serialized.value
                  )
              
              logging.info(f"Produced message {message_count} to Kafka topic {TOPIC_NAME} with key {serialized.key}")

              message_count += 1  
                
              if message_count >= 103000: # 103K

                logging.info(f"Processed {message_count} messages, stopping...")
                return
        

            except websockets.ConnectionClosed as e:
              logging.warning(f"Connection closed: {e}")
              break
            except Exception as e:
              logging.error(f"Error processing message: {e}", exc_info=True)
              continue

      except Exception as e:
        logging.error(f"Connection error: {e}. Reconnecting in 5 seconds...")
        await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(listen_to_bluesky())
    except KeyboardInterrupt:
        logging.info(f"Stopping Producer")