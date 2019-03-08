import json
import re
from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from textblob import TextBlob
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
import sys
import datetime


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


def extract(tweet):
	data = {'text': tweet['text']}
	data['candidate'] = get_candidate(tweet['text'])
	data['sentiment'] = sentiment(tweet['text'])
	return json.dumps(data)


def getElastic():
    return Elasticsearch(['localhost:9200'], timeout=30)

def createIndex(index):
	doc_type = "document"
	esclient = getElastic()
	mapping = {
	  "document": {
	    "properties":{
	      "tweet": {"type": "text"},
	      "candidate": {"type": "text"},
	      "sentiment": {"type": "text"}}}}
	esclient.indices.create(index=index)
	esclient.indices.put_mapping(index = index, doc_type = doc_type, body = mapping)


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
	return statcnt


def process(time, rdd):
	print("========= %s =========" % str(time))
	try:
		sqlContext = getSqlContextInstance(rdd.context)
		df = sqlContext.read.json(rdd)
		#udf_func = udf(lambda x: sentiment(x),returnType=StringType())
		#df = df.withColumn("sentiment",lit(udf_func(df.text)))
		results = df.toJSON().map(lambda j: json.loads(j)).collect()
		#for result in results:
		# 	result["candidate"]=json.loads(get_candidate(result['text']))
		# 	result["sentiment"]=json.loads(result["sentiment"])
		send_elastic(results,"test18","document")
	except:
		pass


def main():
	createIndex("test18")
	sc = SparkContext(appName="PythonStreaming", master="local[2]")
	sqlContext = SQLContext(sc)
	ssc = StreamingContext(sc, 10)
	kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter':1})
	tweets = kafkaStream.map(lambda x: json.loads(x[1]))
	tweets.foreachRDD(process)
	ssc.start()
	ssc.awaitTermination()

if __name__=="__main__":
    main()
