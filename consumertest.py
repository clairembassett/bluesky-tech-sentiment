from quixstreams import Application
import json
import duckdb
import time
from threading import Lock

TARGET = 1000
# DB_PATH = "/Users/clairebassett/gdeltnews.duckdb"   # Move out of OneDrive
DB_PATH = "gdeltnews.duckdb"
# ur just gonna have to add ur specific path to the script when u go to run bc then i cant run it 

TABLE_NAME = "gdelt_articles2"

# intialize variable to keep track of processed articles
# processed_count = 0


    # creat table if doesn't already exist to store articles
def init_duckdb():
    con = duckdb.connect(DB_PATH)

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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
    con = init_duckdb()

    app = Application(
        broker_address="localhost:19092",
        loglevel="DEBUG",
        consumer_group="gdelt_processing",
        auto_offset_reset="earliest",
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["gdelt_cyber"])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")
                continue  

            try:
                key = msg.key().decode("utf-8") if msg.key() else None
                value = json.loads(msg.value().decode("utf-8"))
                offset = msg.offset()

                print(f"Processing: {offset}/{TARGET}, {key}")

                insert_article(con, value)
                con.commit()

                consumer.store_offsets(msg)

            except json.JSONDecodeError as e:
                print(f"WARNING: skipping {offset} because {e}")
                consumer.store_offsets(msg)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping consumer")

