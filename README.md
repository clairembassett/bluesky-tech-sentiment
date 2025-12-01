# Sentiment Analysis on Tech Moguls and their Companies on Bluesky

This project entailed running a sentiment analysis on BlueSky posts, in order to measure the general sentiment regarding certain Tech Moguls and their companies. Highlighting, the overall negative sentiment towards both these companies and their leaders. This project was an assignment titled Data Project 3 completed for DS 3022 - Data Engineering at the University of Virginia, taught by Neal Magee.

This Repository Contains...
1. Bluesky Producer Script - Collects 103,000 Messages from the BlueSky Firehose API using Kafka 
2. Bluesky Consumer Script - Consumes these messages in Kafka and stores them in a DuckDB table
3. Cleaning Script - Cleans the DuckDB table to ensure that only posts are included, removes null values, and filters for posts in English using LangDetect.
4. Sentiment Analysis Script - Filters the dataframe for mentions of tech Moguls and Companies, then utilizes HuggingFaces Transformers - Pipeline to run sentiment analysis on the collected posts, then creates summary tables of the analysis, and visualizations

