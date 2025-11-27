from quixstreams import Application
import json
import time 
import duckdb 

TOPIC_NAME = "blueksy"
GROUP_NAME = "bluesky-con"
DB_PATH = "bluesky.duckdb"
TABLE_NAME = "articles"

def init_duckdb():
    # create duckdb connection 
    con = duckdb.connect(DB_PATH)

    # create table if doesn't already exist to store articles
    # can add source country! 
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            number BIGINT PRIMARY KEY,
            operation TEXT, 
            type TEXT,
            time DATETIME
        );
    """)
    return con

def insert_article(con, article, offset):
    # insert into duckdb 
    # Do NOT specify 'id'; DuckDB will generate it automatically
    con.execute(f"""
        INSERT OR IGNORE INTO {TABLE_NAME} (number, operation, type, time)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (number) DO NOTHING;
    """, [
        offset, # as number
        article["commit"]["operation"], # as operation 
        article["commit"]["record"].get("$type", "unknown"), # as type
        article["commit"]["record"].get("createdAt") # as time
    ])

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

    with app.get_consumer() as consumer:
        consumer.subscribe([TOPIC_NAME])
       
        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")

            if msg.error() is not None:
                raise Exception(msg.error())
            
            try:
                key = msg.key().decode("utf8") if msg.key() else "NoKey" 
                value = json.loads(msg.value().decode("utf-8"))
                offset = msg.offset()

                print(f"Processing: {offset}")

                insert_article(con, value, offset)
                con.commit() 

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