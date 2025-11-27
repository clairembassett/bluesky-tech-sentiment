from quixstreams import Application
import json
import duckdb
import time
from threading import Lock

# DB_PATH = "/Users/clairebassett/gdeltnews.duckdb"   # Move out of OneDrive
DB_PATH = "gdeltnews.duckdb"
# ur just gonna have to add ur specific path to the script when u go to run bc then i cant run it 

TOPIC_NAME = "gdelt6" 
CONSUMER_NAME = "gdelt_processing6"
TABLE_NAME = "gdelt_articles6"

# intialize variable to keep track of processed articles
# processed_count = 0

def init_duckdb():
    # create duckdb connection 
    con = duckdb.connect(DB_PATH)

    # create table if doesn't already exist to store articles
    # can add source country! 
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER,
            koffset BIGINT,
            title TEXT, 
            sourcecountry TEXT, 
            url TEXT UNIQUE, 
            seendate TEXT
        );
    """)
    return con

def insert_article(con, article, offset):
    # insert into duckdb 
    # Do NOT specify 'id'; DuckDB will generate it automatically
    con.execute(f"""
        INSERT OR IGNORE INTO {TABLE_NAME} (koffset, title, sourcecountry, url, seendate)
        VALUES (?, ?, ?, ?, ?);
    """, [
        # i want offset number here as id,
        offset,
        article.get("title"),
        article.get("sourcecountry"),
        article.get("url"),
        article.get("seedate")
    ])
    # processed_count += 1
    # print(f"Success, total processed: {processed_count}")


def main():
    # initialize connection 
    con = init_duckdb() 

    TARGET = 1000
    total_count = 0

    # initialize app 
    app = Application(
        broker_address="localhost:19092",
        loglevel="DEBUG",
        consumer_group=CONSUMER_NAME,
        auto_offset_reset="earliest",
    )

    # consume messages 
    with app.get_consumer() as consumer:
        # topic
        consumer.subscribe([TOPIC_NAME])

        while total_count <= TARGET:
            msg = consumer.poll(1)

            if msg is not None:
                # check for error 
                if msg.error():
                    raise Exception(f"Consumer error: {msg.error()}")

                # process valid message 
                try: 
                    # decode
                    offset = msg.offset() 
                    key = msg.key().decode("utf-8") if msg is None else None

                    if msg.value() is None:
                        print(f"Skipping message at offset {offset}")
                        consumer.store_offsets(msg)
                        continue 

                    value = json.loads(msg.value().decode("utf-8"))
                    

                    print(f"Processing: {offset}")

                    # insert into duckdb
                    insert_article(con, value, offset)
                    # db commit 
                    con.commit() 

                    # commit offset after db commit succeeds
                    consumer.store_offsets(msg)
                    time.sleep(2)
          
                
                except json.JSONDecodeError as e:
                    print(f"WARNING: skipping {offset} because {e}") 
                    consumer.store_offsets(msg) 
            else:
                # runs when consumer.poll(1) returns None
                print("Waiting...")
                time.sleep(2)

            total_count += 1

            if total_count >= TARGET:
                    break

        # done consuming
        print("\n Finished consuming")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping consumer")
