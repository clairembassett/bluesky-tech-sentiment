from quixstreams import Application
import json
import duckdb
import time

DB_PATH = "gdeltnews.duckdb"
TABLE_NAME = "gdelt_articles"

# Track processed articles
processed_count = 0


# -------------------------------------------
# Initialize DuckDB
# -------------------------------------------
def init_duckdb():
    con = duckdb.connect(DB_PATH)

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            author TEXT,
            title TEXT,
            description TEXT,
            url TEXT UNIQUE,
            publishedAt TIMESTAMP
        );
    """)

    return con


# -------------------------------------------
# Insert a single article
# -------------------------------------------
def insert_article(con, article):
    global processed_count

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

    processed_count += 1
    print(f"Inserted → total processed: {processed_count}")


# -------------------------------------------
# Main Kafka consumer loop
# -------------------------------------------
def main():
    con = init_duckdb()

    app = Application(
        broker_address="localhost:19092",
        loglevel="DEBUG",
        consumer_group="gdelt_processing_group",
        auto_offset_reset="earliest",
    )

    with app.get_consumer() as consumer:

        consumer.subscribe(["gdelt_articles"])
        print("Consumer running... waiting for messages.")

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")
                continue

            try:
                key = msg.key().decode("utf-8") if msg.key() else None
                value = json.loads(msg.value().decode("utf-8"))
                offset = msg.offset()

                print(f"Processing offset {offset}, key={key}")

                insert_article(con, value)
                con.commit()

                consumer.store_offsets(msg)

                time.sleep(1)

            except Exception as e:
                print(f"Error processing message: {e}")
                time.sleep(2)


# -------------------------------------------
# Entry point
# -------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping consumer...")
