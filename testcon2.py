from quixstreams import Application
import json
import duckdb
import time

DB_PATH = "gdeltnews.duckdb"
TABLE_NAME = "gdelt4"

def main():

    Target = 1000
    total_count = 0

    # Initialize DuckDB connection once
    con = duckdb.connect(DB_PATH)

    # Make sure table exists
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            author TEXT,
            title TEXT,
            description TEXT,
            url TEXT UNIQUE,
            publishedAt TIMESTAMP
        )
    """)

    app = Application(
        broker_address="localhost:19092",
        consumer_group="gdelt_splitter_group",
        auto_offset_reset="earliest",
        loglevel="DEBUG",
    )

    print("DuckDB consumer running...")

    with app.get_consumer() as consumer:
        consumer.subscribe(["gdelt_cyber"])

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            total_count += 1

            # The actual article data from Kafka
            data = json.loads(msg.value().decode("utf-8"))

            # Insert real values
            con.execute(f"""
                INSERT OR IGNORE INTO {TABLE_NAME} (author, title, description, url, publishedAt)
                VALUES (?, ?, ?, ?, ?);
            """, [
                data.get("author"),
                data.get("title"),
                data.get("description"),
                data.get("url"),
                data.get("publishedAt")
            ])

            print(f"Inserted message {total_count}")

            # Stop at target
            if total_count >= Target:
                print(f"Reached target of {Target}. Stopping consumer.")
                break

    # Close DB cleanly
    con.close()
    print("DuckDB connection closed.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping consumer")


