from quixstreams import Application
import json
import duckdb
import time
from threading import Lock

# DB_PATH = "/Users/clairebassett/gdeltnews.duckdb"   # Move out of OneDrive
DB_PATH = "gdeltnews.duckdb"
# ur just gonna have to add ur specific path to the script when u go to run bc then i cant run it 

TABLE_NAME = "gdelt_articles"

# intialize variable to keep track of processed articles
# processed_count = 0

def init_duckdb():
    # create duckdb connection 
    con = duckdb.connect(DB_PATH)

    # creat table if doesn't already exist to store articles
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY,
            author TEXT, 
            title TEXT, 
            description TEXT,
            url TEXT UNIQUE, 
            publishedAt TIMESTAMP
        );
    """)
    return con

def insert_article(con, article):
    # insert into duckdb 
    # Do NOT specify 'id'; DuckDB will generate it automatically
    con.execute(f"""
        INSERT OR IGNORE INTO {TABLE_NAME} (author, title, description, url, publishedAt)
        VALUES (?, ?, ?, ?, ?);
    """, [
        article.get("author"),
        article.get("title"),
        article.get("description"),
        article.get("url"),
        article.get("publishedAt")
    ])
    # processed_count += 1
    # print(f"Success, total processed: {processed_count}")


def main():
    # initialize connection 
    con = init_duckdb() 

    # initialize app 
    app = Application(
        broker_address="localhost:19092",
        loglevel="DEBUG",
        consumer_group="gdelt_processing",
        auto_offset_reset="earliest",
    )

    # consume messages 
    with app.get_consumer() as consumer:
        # topic
        consumer.subscribe(["gdelt_cyber"])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")
            
            try: 
                # decode
                key = msg.key().decode("utf-8") if msg is None else None
                value = json.loads(msg.value().decode("utf-8"))
                offset = msg.offset() 

                print(f"Processing: {offset}, {key}")

                # insert into duckdb
                insert_article(con, value)
                # db commit 
                con.commit() 

                # commit offset after db commit succeeds
                consumer.store_offsets(msg)
                time.sleep(5)
                
            except json.JSONDecodeError as e:
                print(f"WARNING: skipping {offset} because {e}") 
                consumer.store_offsets(msg) 


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping consumer")
