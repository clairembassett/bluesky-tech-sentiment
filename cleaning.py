# Cleaning Script
import duckdb
import pandas as pd
from langdetect import detect, DetectorFactory
import logging 


# Set up logging
logging.basicConfig(level=logging.INFO)

# Connecting to duckdb
con = duckdb.connect("bluesky-posts.duckdb")


# Loading data and printing out the total rows(the total number of posts)
df = con.execute("SELECT * FROM posts").fetchdf()
print(f"Loaded {len(df)} rows from Bluesky DB.")

# Ensuring that our data only contains posts, by filtering operation = create and type = post
df = df[(df['operation'] == 'create') & (df['type'] == 'app.bsky.feed.post')]
logging.info(f"{len(df)} rows remain after filtering operation and type")

#Deleting these columns after ensuring that the df only contains posts
cols_to_drop = ['operation', 'type'] 
df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

# Deleting rows with no value in the text column
df['text'] = df['text'].astype(str) 
df = df[df['text'].str.strip() != ""]
logging.info(f"{len(df)} rows remain after removing empty text")

# Using the langdetect package to ensure that all posts are in English
DetectorFactory.seed = 0

def is_english(text):
    try:
        return detect(text) == 'en'
    except:
        return False

df = df[df['text'].apply(is_english)]
logging.info(f"{len(df)} rows remain after keeping only English text")


logging.info(f"After Cleaning: {len(df)} rows remain and number of columns remain {len(df.columns)}")

# Saving the new df in duckdb
con.register("cleanddf", df)

# Save as a new table in DuckDB
con.execute("CREATE TABLE IF NOT EXISTS blueskyclean AS SELECT * FROM cleanddf")
logging.info(f"Cleaned Dataframe saved as blueskyclean in DuckDB!")





