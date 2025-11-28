from quixstreams import Application
import json
import time 
import duckdb 

TOPIC_NAME = "bluesky5"
GROUP_NAME = "bluesky-con"
DB_PATH = "bluesky.duckdb"
TABLE_NAME = "blueskydb"

def init_duckdb():

    con = duckdb.connect(DB_PATH) #Create DuckDB Connection

    #Creates table to store posts if it does not exist
    con.execute(f""" 
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} ( 
            number BIGINT PRIMARY KEY,
            operation TEXT,
            type TEXT,
            createdAt TEXT,
            text TEXT
        );
    """)
    return con

# Inserts in to duckdb
def insert_post(con, value, offset):
    commit = value.get("commit", {}) #Uses the original method but in two different parts
    record = commit.get("record")

    # Only processes post and posts with records
    if not record or record.get("$type") != "app.bsky.feed.post":
        return False

    operation = commit.get("operation")
    createdAt = record.get("createdAt")
    text = record.get("text", "")

    con.execute(f"""
        INSERT INTO {TABLE_NAME} (number, operation, type, createdAt, text)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (number) DO NOTHING;
    """, [
        offset,
        operation,
        record.get("$type"),
        createdAt,
        text
    ])
    return True

def main():
    con = init_duckdb()

    app = Application(
        broker_address="127.0.0.1:19092",
        loglevel="INFO",
        consumer_group=GROUP_NAME,
        auto_offset_reset="earliest",
        producer_extra_config={
            "broker.address.family": "v4",
        }
    )

    Target = 102000
    count = 0

    with app.get_consumer() as consumer:
        consumer.subscribe([TOPIC_NAME])
       
        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")
                continue

            if msg.error() is not None:
                raise Exception(msg.error())
            
            try:
                key = msg.key().decode("utf8") if msg.key() else "NoKey" 
                value = json.loads(msg.value().decode("utf-8"))
                offset = msg.offset()

                print(f"Processing: {offset}")

                inserted = insert_post(con, value, offset)

                # Adds to count if post was inserted
                if inserted:
                    count += 1
                    print(f"Inserted post {count}/{Target}")

                    if count >= Target:
                        print(f"Reached target of {Target} posts!")
                        break  # Exit the loop


                consumer.store_offsets(msg)
                time.sleep(2) 

            except Exception as e:
                print(f"Error processing message: {e}")
                time.sleep(2)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping consumer")