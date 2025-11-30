# Cyber-Vulnerability-Monitor-DP3-DS3022

1. Kafka producer to tap into news api.
2. Kafka consumer that iterates through each message and processes them as individual messages.
3. Kafka producer 2 with second topic that consumes each individual news info as an individual message.
4. Kafka consumer 2 that takes messages into second consumer group.


Iliana notes from 11/19:
- order for running scripts is producer-test -> con-prod -> second-consumer
- each time I do a new run/test I just make a new table in the same db, so rn there's 3 tables and my red pandas has 3 different topics/consumer groups
- bad news: we are losing some data. idk where, but i know that for the most recent one there was ~5700 total results in topic, but only 294 messages in the final consumer group & duckdb table.

Iliana from 11/20:
- tried to update con-prod.py to not lose as many data points but didn't work. 
- main thing is got rid of the processing gurantee of exactly-once bc apparently manually polling and commiting offsets violated the exactly-once gurantee and can cause issues. however doing so didn't get rid of our data lose problem...


Claire 11/21
- Going to retry our polling methods to do the for loop, because there are international articles coming up, plus will allow us for us to parse more?
- Will label scripts producertest2, con-prod2, and second-consumer2
- Little bit worried bc I can only make 100 requests per 24 hrs 
- Switched to the gdelt api link here: https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
- Set counter for producer to 1000, to test for leaky pipeline
- Messages are recieved per article 
- con-prod2 needs to have a break at 1000 articles(for now)
- keep getting this error message for con-prod2 Constraint Error: NOT NULL constraint failed: gdelt_articles.id time for mallard!!


started running around 5 pm 11/27

Iliana 11/29
- renamed and added comments to sentiment.py
- created other-sentiment.py for a second analysis? I think we could do something abt the economy, or we could relate it to student loans/debt or job hiring bc that is very applicable to us
- added code to save the output jpgs to the repo