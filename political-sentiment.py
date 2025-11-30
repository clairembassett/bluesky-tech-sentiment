import duckdb
import pandas as pd
from transformers import pipeline
import seaborn as sns
import matplotlib.pyplot as plt

# connect to DuckDB
con = duckdb.connect("bluesky.duckdb")

# ***************************
# Make pandas dataframe 
# only pulling rows that include names/terms we want to analyze
df = con.execute("""
    SELECT number, text, createdAt
    FROM blueskyclean
    WHERE text ILIKE '%trump%' OR text ILIKE '%biden%' 
       OR text ILIKE '%liberal%' OR text ILIKE '%conservative%'
""").fetchdf()

# ensure timestamp is datetime
df['createdAt'] = pd.to_datetime(df['createdAt'], format='mixed', utc=True)

# function that makes all text in same text format for analyzing
def assign_figure(text):
    text_lower = text.lower()
    if 'trump' in text_lower:
        return 'Trump'
    elif 'biden' in text_lower:
        return 'Biden'
    elif 'liberal' in text_lower:
        return 'Liberal'
    elif 'conservative' in text_lower:
        return 'Conservative'
    else:
        return 'Other'

# create new column where prev function is applied
df['figure'] = df['text'].apply(assign_figure)

# ***************************
# Sentiment analysis 
print("Running sentiment analysis (may take a few minutes)...")
# create variable for sentiment analyzer 
sentiment_analyzer = pipeline("sentiment-analysis")

# create new column for sentiment label
df['sentiment'] = df['text'].apply(lambda x: sentiment_analyzer(x)[0]['label'])
# create new column for sentiment score 
df['score'] = df['text'].apply(lambda x: sentiment_analyzer(x)[0]['score'])

# summarizing 
sentiment_summary = df.groupby(['figure', 'sentiment']).size().reset_index(name='count')
print("\nSentiment Summary:")
print(sentiment_summary)

# ----------------------------
# Plotting 
plt.figure(figsize=(10,6))
# make barplot w sentiment label on x axis and count on y
# colored by negative vs positive sentiment 
sns.barplot(data=sentiment_summary, x='figure', y='count', hue='sentiment')
# make title 
plt.title("Bluesky Posts Sentiment by Political Figure")
# add axis labels
plt.ylabel("Number of Posts")
plt.xlabel("Political Figure")
# don't rotate the axis labels
plt.xticks(rotation=0)
plt.tight_layout()
# save plot output to jpg in repo 
plt.savefig("political_sentiment.jpg", format="jpg", dpi=300)
print("\nPlot saved to repo.")
# show plot 
plt.show()


# ----------------------------
# save results to csv in repo 
df.to_csv("bluesky_sentiment_results.csv", index=False)
print("\nResults saved to bluesky_sentiment_results.csv")
