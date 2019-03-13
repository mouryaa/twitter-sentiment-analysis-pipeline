import json
from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
import re
from textblob import TextBlob


def clean_tweet(tweet):
	return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

def sentiment(tweet):
	analysis = TextBlob(clean_tweet(tweet))
	if analysis.sentiment.polarity > 0:
		return 'positive'
	elif analysis.sentiment.polarity == 0:
		return 'neutral'
	else:
		return 'negative'

def get_candidate(text):
	if "kamala" in text.lower():
		return "kamala"
	elif "biden" in text.lower():
		return "biden"
	elif "bernie" in text.lower():
		return "bernie"
	else:
		return "other"


def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


def getElastic():
    return Elasticsearch(['localhost:9200'], timeout=30)

def createIndex(index):
	try:
		doc_type = "document"
		esclient = getElastic()
		mapping = {"document": {"properties":{"tweet": {"type": "text"},"candidate": {"type": "text"},"sentiment": {"type": "text"}}}}
		esclient.indices.create(index=index)
		esclient.indices.put_mapping(index = index, doc_type = doc_type, body = mapping)
	except:
		pass

def send_elastic(doc, index, type):
	esclient = getElastic()
	i = 0
	actions = []
	for row in doc:
		actions.append({"_op_type": "index","_index": index,"_type": type,"_source": row})
	for ok, response in helpers.streaming_bulk(esclient, actions, index=index, doc_type=type,max_retries=5,raise_on_error=False, raise_on_exception=False):
	    if not ok:
	        i+=0
	        print(response)
	    else:
	        i+=1
	return i

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
		sqlContext = getSqlContextInstance(rdd.context)
		df = sqlContext.read.json(rdd,multiLine=True)
		cand = udf(lambda x: get_candidate(x))
		sent = udf(lambda x: sentiment(x))
		df = df.withColumn("candidate",cand(df.text))
		df = df.withColumn("sentiment",sent(df.text))
		tweetsentiments = df.toJSON().map(lambda x: json.loads(x)).collect()
		send_elastic(tweetsentiments,"index2","document")
    except:
        pass


def main():
	createIndex("index2")
	sc = SparkContext(appName="twitterstream", master="local[2]")
	sqlContext = SQLContext(sc)
	ssc = StreamingContext(sc, 10)
	kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter':1})
	tweets = kafkaStream.map(lambda x: json.loads(x[1]))
	tweets.foreachRDD(process)
	ssc.start()
	ssc.awaitTermination()

if __name__=="__main__":
    main()
