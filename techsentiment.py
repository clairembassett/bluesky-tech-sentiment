import duckdb
import pandas as pd
from transformers import pipeline
import seaborn as sns
import matplotlib.pyplot as plt
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Connecting to DuckDB
con = duckdb.connect("bluesky-posts.duckdb")
logging.info("Connected to DuckDB!")

# Pulling the relevant information of tech moguls and their companies, by filtering the data frame
# Moguls chosen are Sam Altman, Mark Zuckerberg, Elon Musk, Bill Gates, and Sundar Pichai
# Respective companies chosen are OpenAI, Amazon, Meta, Tesla/SpaceX/X, Microsoft, Google
df = con.execute("""
    SELECT number, text, createdAt
    FROM blueskyclean 
    WHERE 
        -- Moguls
                 
        text ILIKE '%altman%' OR 
        text ILIKE '%sam altman%' OR
        text ILIKE '%sama%' OR

        text ILIKE '%bezos%' OR 
        text ILIKE '%jeff bezos%' OR

        text ILIKE '%zuck%' OR 
        text ILIKE '%zuckerberg%' OR 
        text ILIKE '%mark zuckerberg%' OR

        text ILIKE '%musk%' OR 
        text ILIKE '%elon musk%' OR 
        text ILIKE '%elon%' OR

        text ILIKE '%gates%' OR 
        text ILIKE '%bill gates%' OR

        text ILIKE '%pichai%' OR 
        text ILIKE '%sundar pichai%' OR
        text ILIKE '%sundar%'


        -- Companies
        OR text ILIKE '%openai%' OR text ILIKE '%chatgpt%' OR
        text ILIKE '%amazon%' OR text ILIKE '%aws%' OR
        text ILIKE '%meta%' OR text ILIKE '%facebook%' OR
        text ILIKE '%instagram%' OR text ILIKE '%whatsapp%' OR
        text ILIKE '%tesla%' OR text ILIKE '%spacex%' OR
        text ILIKE '%x.com%' OR text ILIKE '%twitter%' OR
        text ILIKE '%microsoft%' OR text ILIKE '%copilot%' OR text ILIKE '%azure%' OR text ILIKE '%bing%' OR
        text ILIKE '%google%' OR text ILIKE '%gemini%' OR text ILIKE '%deepmind%'       
""").fetchdf()


# Mapping keywords to ensure that results include all references to mogul
mogul_keywords = {
    "Altman": ["altman", "sam altman", "sama"],
    "Bezos": ["bezos", "jeff bezos"],
    "Zuckerberg": ["zuck", "zuckerberg", "mark zuckerberg"],
    "Musk": ["musk", "elon musk", "elon"],
    "Gates": ["gates", "bill gates"],
    "Pichai": ["sundar", "pichai"],
}

# Mapping keywords to ensure that results include all references to company
company_keywords= {
    "OpenAI": ["openai", "chatgpt"],
    "Amazon": ["amazon", "aws"],
    "Meta": ["meta", "facebook", "instagram", "whatsapp"],
    "Tesla": ["tesla", "spacex", "x.com", "twitter"],
    "Microsoft": ["microsoft", "azure", "copilot", "bing"],
    "Google": ["google", "gemini", "deepmind"]
}

# Mapping each keyword to its specific mogul
def detect_mogul(text):
    tl = text.lower()
    for mogul, keys in mogul_keywords.items():
        if any(k in tl for k in keys):
            return mogul
    return None

# Mapping each keyword to its specific company 
def detect_company(text):
    tl = text.lower()
    for comp, keys in company_keywords.items():
        if any(k in tl for k in keys):
            return comp
    return None

# Applying the mapping to the dataframe
df["mogul"] = df["text"].apply(detect_mogul)
df["company"] = df["text"].apply(detect_company)

# Only keeping the rows where mogul and company match
df = df[(df["mogul"].notna()) | (df["company"].notna())]

# Runnning the sentiment analysis with the transformers package
logging.info("Running sentiment analysis!")
sentiment_analyzer = pipeline("sentiment-analysis")

df["sentiment"] = df["text"].apply(lambda x: sentiment_analyzer(x)[0]["label"])
df["score"] = df["text"].apply(lambda x: sentiment_analyzer(x)[0]["score"])

# Printing summary tables of sentiment towards mogul
logging.info("\n### Sentiment Toward Moguls ###")
mogul_summary = df.groupby(["mogul", "sentiment"]).size().reset_index(name="count")
logging.info(mogul_summary)

# Printing summary tables of sentiement towards company
print("\n### Sentiment Toward Companies ###")
company_summary = df.groupby(["company", "sentiment"]).size().reset_index(name="count")
print(company_summary)

# Creating dict to make sentiments the same color on the bar graph 
sentiment_palette = {
    "POSITIVE": "#ff8a05", # orange
    "NEGATIVE": "#2a1dd3" # dark blue 
}

# Plotting sentiment towards tech moguls
plt.figure(figsize=(12,6))

# Listing moguls 
moguls = ["Altman", "Bezos", "Gates", "Musk", "Pichai", "Zuckerberg"]

# Using seaborn to make a barplot, with tech mogul on x axis and count of posts on y axis
# Using hue to separate by sentiment, negative vs positive
# Ordering by moguls
# Using the palette to color the bars specifically
ax = sns.barplot(data=mogul_summary, x="mogul", y="count", hue="sentiment", order=moguls, palette=sentiment_palette, width=0.7, dodge=True)

# Adding legend to the plot
plt.legend() 

# adding title and axis labels
plt.title("Sentiment Toward Tech Moguls on Bluesky")
plt.xlabel("Tech Mogul")
plt.ylabel("Number of Posts")
plt.xticks(rotation=0)
plt.tight_layout()

# saving plot as jpg to repo
plt.savefig("mogul_sentiment.jpg", dpi=300)
# Only using plt.show() for testing, not for submitting
# plt.show()

# Plotting sentiment towards companies
plt.figure(figsize=(12,6))
# Listing companies in order of their respective moguls
companies = ["OpenAI", "Amazon", "Microsoft", "Tesla", "Google", "Meta"]

# Using hue to separate by sentiment, negative vs positive
# Ordering by moguls
# Using the palette to color the bars specifically
sns.barplot(data=company_summary, x="company", y="count", hue="sentiment", order=companies, palette=sentiment_palette, width=0.7, dodge=True)

# Adding legend 
plt.legend() 

# Adding title and axis labels 
plt.title("Sentiment Toward Moguls' Companies on Bluesky")
plt.xlabel("Company")
plt.ylabel("Number of Posts")
plt.xticks(rotation=0)
plt.tight_layout()

# saving plot as jpg to repo 
plt.savefig("company_sentiment.jpg", dpi=300)
# Only using plt.show() for testing not for submitting 
# plt.show()

# Saving final outputs - CSV file and graph JPG
df.to_csv("mogul_company_sentiments.csv", index=False)

logging.info("\nSaved cleaned and analyzed results to mogul_company_sentiments.csv")
logging.info("Plots saved as mogul_sentiment.jpg and company_sentiment.jpg")
