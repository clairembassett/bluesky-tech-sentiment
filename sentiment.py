# ------------------------------------------------------------
# bluesky_sentiment_analysis.py
# ------------------------------------------------------------
import duckdb
import pandas as pd
from transformers import pipeline
import seaborn as sns
import matplotlib.pyplot as plt

# ----------------------------
# 1. Connect to DuckDB
# ----------------------------
con = duckdb.connect("bluesky.duckdb")

# ----------------------------
# 2. Extract posts mentioning political figures
# ----------------------------
df = con.execute("""
    SELECT number, text, createdAt
    FROM blueskydb
    WHERE text ILIKE '%trump%' OR text ILIKE '%biden%' 
       OR text ILIKE '%liberal%' OR text ILIKE '%conservative%'
""").fetchdf()

# Ensure timestamp is datetime
df['createdAt'] = pd.to_datetime(df['createdAt'], format='mixed', utc=True)

# ----------------------------
# 3. Assign political figure to each post
# ----------------------------
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

df['figure'] = df['text'].apply(assign_figure)

# ----------------------------
# 4. Sentiment Analysis
# ----------------------------
print("Running sentiment analysis (may take a few minutes)...")
sentiment_analyzer = pipeline("sentiment-analysis")

# Apply sentiment analysis (this can take time on large datasets)
df['sentiment'] = df['text'].apply(lambda x: sentiment_analyzer(x)[0]['label'])
df['score'] = df['text'].apply(lambda x: sentiment_analyzer(x)[0]['score'])

# ----------------------------
# 5. Summarize sentiment counts
# ----------------------------
sentiment_summary = df.groupby(['figure', 'sentiment']).size().reset_index(name='count')
print("\nSentiment Summary:")
print(sentiment_summary)

# ----------------------------
# 6. Plot sentiment counts by figure
# ----------------------------
plt.figure(figsize=(10,6))
sns.barplot(data=sentiment_summary, x='figure', y='count', hue='sentiment')
plt.title("Bluesky Posts Sentiment by Political Figure")
plt.ylabel("Number of Posts")
plt.xlabel("Political Figure")
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()

# ----------------------------
# 7. Optional: save results
# ----------------------------
df.to_csv("bluesky_sentiment_results.csv", index=False)
print("\nResults saved to bluesky_sentiment_results.csv")
