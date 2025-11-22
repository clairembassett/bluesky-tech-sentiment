from quixstreams import Application
import json
import duckdb

DB_PATH = "gdeltnews.duckdb"
TABLE_NAME = "gdelt_articles"

class ArticleSaver:
    """
    A class to manage the database connection and insertion of articles.
    """
    def __init__(self):
        self.con = self._init_duckdb()
        self.processed_count = 0

    def _init_duckdb(self):
        """Initialize DuckDB connection and table."""
        con = duckdb.connect(DB_PATH)
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

    def insert_article(self, article: dict):
        """Insert a single article into DuckDB."""
        # Do NOT specify 'id'; DuckDB will generate it automatically
        self.con.execute(f"""
            INSERT OR IGNORE INTO {TABLE_NAME} (author, title, description, url, publishedAt)
            VALUES (?, ?, ?, ?, ?);
        """, [
            article.get("author"),
            article.get("title"),
            article.get("description"),
            article.get("url"),
            article.get("publishedAt")
        ])
        self.con.commit()
        self.processed_count += 1
        print(f"Processed article, total processed: {self.processed_count}")

    def close(self):
        """Close the DB connection."""
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

    saver = ArticleSaver()

    # Apply the processing function to each message from the stream
    sdf.apply(saver.insert_article)

    print("DuckDB consumer running...")
    try:
        app.run()
    finally:
        # Ensure the database connection is closed gracefully on exit
        saver.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping DuckDB consumer...")