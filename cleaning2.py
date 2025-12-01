# Cleaning Script
import duckdb
import pandas as pd
from langdetect import detect, DetectorFactory
import logging 

from multiprocessing import Process, Manager 
TIMEOUT = 5 # seconds

# Set up logging
logging.basicConfig(level=logging.INFO)

# Using the langdetect package to ensure that all posts are in English
DetectorFactory.seed = 0

# Connecting to duckdb
con = duckdb.connect("bluesky-posts.duckdb")

# Loading data and printing out the total rows(the total number of posts)
df = con.execute("SELECT * FROM posts").fetchdf()
print(f"Loaded {len(df)} rows from Bluesky DB.")

# Ensuring that our data only contains posts, by filtering operation = create and type = post
df = df[(df['operation'] == 'create') & (df['type'] == 'app.bsky.feed.post')]
logging.info(f"{len(df)} rows remain after filtering operation and type")

# Deleting these columns after ensuring that the df only contains posts
cols_to_drop = ['operation', 'type'] 
df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

# Deleting rows with no value in the text column
df['text'] = df['text'].astype(str) 
df = df[df['text'].str.strip() != ""]
logging.info(f"{len(df)} rows remain after removing empty text")

# Helper function to run detection logic 
def detect_english(text, return_dict):
    # stores True/False result in a dict 
    try:
        result = detect(text) == 'en'
        return_dict['result'] = result
    except Exception as e:
        return_dict['result'] = False

def is_english_w_timeout(text):
    if pd.isna(text) or not str(text).strip():
        return False 

    # Use manager to share results between processes 
    manager = Manager() 
    return_dict = manager.dict()

    # Create process to target detect_english function 
    p = Process(target=detect_english, args=(text, return_dict))
    # Start the process
    p.start() 

    # Wait the number of timout seconds 
    p.join(TIMEOUT)

    # Check if process is still running 
    if p.is_alive():
        logging.info(f"Warning: Detection timed out for text: '{str(text)[:50]}...'")
        # Stop the process
        p.terminate()
        # Wait for termination to be done 
        p.join() 
        # Return False bc timed out, which assumes non-English or invalid
        return False 

    # Return the result from the dict 
    return return_dict.get('result', False)
    
# Create list to store T/F result for filtering later 
en_filter = []
# For each row in the df, 
for row in df.itertuples():
    # Log the row index to check on process 
    logging.info(f"Processing row: {row.Index}")
    # Pull value of the text column of current row 
    text_content = row.text
    # Apply is_english_w_timeout function to the text content 
    is_en = is_english_w_timeout(text_content)
    # Append result to the list 
    en_filter.append(is_en)

# Apply filtered mask to the df
df = df[en_filter]
# Log number of rows remaining after keeping only English text
logging.info(f"{len(df)} rows remain after keeping only English text")

# Log number of rows remaining after cleaning 
logging.info(f"After Cleaning: {len(df)} rows remain and number of columns remain {len(df.columns)}")

# Saving the new df in duckdb
con.register("cleanddf", df)

# Save as a new table in DuckDB
con.execute("CREATE TABLE IF NOT EXISTS blueskyclean AS SELECT * FROM cleanddf")
logging.info(f"Cleaned Dataframe saved as blueskyclean in DuckDB!")





