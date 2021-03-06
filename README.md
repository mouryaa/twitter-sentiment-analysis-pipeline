# Twitter Sentiment Analysis Pipeline

## Objective
The goal of this project was to build a real time streaming data pipeline to read tweets from Twitter and apply sentiment analysis on those tweets.

## Architecture

   </br><img src="/docs/architecture.png" width="800" height=auto />

The kafka producer starts retrieving tweets using the Twitter API for the terms that I entered and it is consumed by Spark where they are processed and sent to elatsticsearch and can be viewed in Kibana.

## Steps 

Here the steps to get the project working:

1. Start zookeeper
```
zkServer start
```
2. Start kafka
```
kafka-server-start /usr/local/etc/kafka/server.properties
```
3. Start producing tweets
```
python twitterproducer.py
```
4. Run script to process tweets
```
spark-submit --jars '/paths to jars' ./processtweets.py
```
 
