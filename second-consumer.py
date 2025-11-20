from quixstreams import Application
import json
import time
import duckdb 

# SECOND CONSUMER THAT PROCESSES EACH ARTICLE AS INDIVIDUAL MESSAGE AND SAVES TO DUCKDB 

def init_duckdb():
    # create connection 
    con = duckdb.connect("news.duckdb")

    # create table if not exists to store articles 
    con.execute("""
        CREATE TABLE IF NOT EXISTS processed_news4 (
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
        INSERT INTO processed_news4 (author, title, description, url, publishedAt)
        VALUES (?, ?, ?, ?, ?);
    """, [
        article.get("author"),
        article.get("title"),
        article.get("description"),
        article.get("url"),
        article.get("publishedAt")
    ]) 

def main():
    # initialize connection 
    con = init_duckdb() 

    # initialize app 
    app = Application(
        broker_address="localhost:19092",
        loglevel="DEBUG",
        # make second new consumer group
        consumer_group="news_processing4",
        # use default at-least-once processing guarantees: 
        # processing_guarantee="exactly-once",
        auto_offset_reset="earliest",
    )

    # consume messages 
    with app.get_consumer() as consumer:
        # second topic 
        consumer.subscribe(["news_articles4"])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")

            if msg.error():
                raise Exception(msg.error())

            try: 
                # decode 
                key = msg.key().decode("utf8") if msg.key() else None 
                value = json.loads(msg.value().decode("utf-8"))
                offset = msg.offset()

                print(f"Processing: {offset}, {key}")

                # insert into duckdb
                insert_article(con, value)
                # db commit 
                con.commit() 
                
                # commit offset after db commit succeeds
                consumer.store_offsets(msg)
                time.sleep(1)

            except json.JSONDecodeError as e:
                print(f"WARNING: skipping {offset} because {e}") 
                consumer.store_offsets(msg)

                

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping consumer")
