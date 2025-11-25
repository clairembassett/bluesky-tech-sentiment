from quixstreams import Application
import json
import duckdb
import time
from threading import Lock

DB_PATH = "/Users/clairebassett/gdeltnews.duckdb"   # Move out of OneDrive
TABLE_NAME = "gdelt_articles"


class ArticleSaver:
    """
    Handles DuckDB connection + batched inserts with retry logic.
    """

    def __init__(self, batch_size=50, retry_wait=0.2, retry_max=20):
        self.batch_size = batch_size
        self.retry_wait = retry_wait
        self.retry_max = retry_max
        self.buffer = []
        self.lock = Lock()

        self.con = self._init_duckdb()
        self.processed_count = 0

    def _init_duckdb(self):
        """Initialize DuckDB connection and ensure table exists."""
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

    def _flush_batch(self):
        """Flush buffered rows to DuckDB with retry logic."""
        if not self.buffer:
            return

        retry_attempts = 0

        while True:
            try:
                self.con.executemany(f"""
                    INSERT OR IGNORE INTO {TABLE_NAME}
                    (author, title, description, url, publishedAt)
                    VALUES (?, ?, ?, ?, ?);
                """, self.buffer)

                self.buffer.clear()
                return

            except duckdb.IOException as e:
                # Database is locked
                retry_attempts += 1
                if retry_attempts >= self.retry_max:
                    print("❌ Failed after max retries:", e)
                    return

                print(f"⚠️ DuckDB locked, retrying... ({retry_attempts})")
                time.sleep(self.retry_wait)

    def insert_article(self, article: dict):
        """Queue article for batch insert."""
        with self.lock:
            row = [
                article.get("author"),
                article.get("title"),
                article.get("description"),
                article.get("url"),
                article.get("publishedAt"),
            ]

            self.buffer.append(row)

            if len(self.buffer) >= self.batch_size:
                self._flush_batch()

            self.processed_count += 1
            print(f"Processed articles: {self.processed_count}", end="\r")

    def close(self):
        """Final batch flush + close connection."""
        print("\nFlushing remaining articles...")
        self._flush_batch()

        self.con.close()
        print("DuckDB connection closed.")


def main():
    app = Application(
        broker_address="localhost:19092",
        loglevel="DEBUG",
        consumer_group="gdelt_processing_group",
        auto_offset_reset="earliest",
    )

    input_topic = app.topic("gdelt_articles", value_deserializer="json")
    sdf = app.dataframe(input_topic)

    saver = ArticleSaver(batch_size=50)

    sdf.apply(saver.insert_article)

    print("DuckDB consumer running...")

    try:
        app.run()
    finally:
        saver.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopping DuckDB consumer...")
