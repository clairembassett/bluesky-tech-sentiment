from quixstreams import Application
import json
import time
import duckdb 

# SECOND CONSUMER THAT PROCESSES EACH ARTICLE AS INDIVIDUAL MESSAGE AND SAVES TO DUCKDB 

def init_duckdb():
    # create connection 
    con = duckdb.connect("news.duckdb")

    con.execute("""
        CREATE TABLE IF NOT EXISTS processed_news3 (
            id INTEGER,
            author TEXT, 
            title TEXT, 
            description TEXT,
            url TEXT, 
            publishedAT DATETIME
        );
    """)
    return con 

def insert_article(con, article):
    # insert into duckdb
    con.execute("""
        INSERT INTO processed_news3 (author, title, description, url, publishedAt)
        VALUES (?, ?, ?, ?, ?);
    """, [
        article.get("author"),
        article.get("title"),
        article.get("description"),
        article.get("url"),
        article.get("publishedAt")
    ]) 

def main():
    con = init_duckdb() 

    app = Application(
        broker_address="localhost:19092",
        # set the logging level
        loglevel="DEBUG",
        # define the consumer group
        consumer_group="news_processing3",
        # processing guarantees: 
        #   - exactly-once   - msg will be processe exactly once
        processing_guarantee="exactly-once",
        # set the offset reset - either earliest || latest
        auto_offset_reset="earliest",
    )

# Offset
    with app.get_consumer() as consumer:
        consumer.subscribe(["news_articles3"])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode("utf8") if msg.key() else None 
                value = json.loads(msg.value())
                offset = msg.offset()

                print(f"{offset} {key} {value}")

                insert_article(con, value)
                con.commit() 


                consumer.store_offsets(msg)
                time.sleep(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
