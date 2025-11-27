import asyncio
import websockets
from quixstreams import Application
import os
import json

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
uri = "wss://jetstream2.us-west.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
TOPIC_NAME = "bluesky5"

# async means python, u do this on ur own time if u need to buffer it off for  few milliseconds 
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
          print("Connected to Bluesky firehose...")
          while True:
            try:
              message = await websocket.recv()
              data = json.loads(message)
              print(data)

             
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

              message_count += 1  
              if message_count >= 1000:
                print(f"Processed {message_count} messages, stopping...")
                return
        
              # print(serialized)

            except websockets.ConnectionClosed as e:
              print(f"Connection closed: {e}")
              break
            except Exception as e:
              print(f"Error processing message: {e}")
              continue

      except Exception as e:
        print(f"Connection error: {e}. Reconnecting in 5 seconds...")
        await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(listen_to_bluesky())
    except KeyboardInterrupt:
        print("\n\nStopping stream...")