import json
import tweepy
from tweepy.streaming import StreamListener
from kafka import KafkaProducer
from kafka import SimpleProducer, KafkaClient
import twitter_config


def datasend(data):
    rawtweet = json.loads(data)
    tweet={}
    tweet["text"] = rawtweet["text"]
    return json.dumps(tweet)

class TwitterListener(StreamListener):
    def __init__(self, api):
        self.api = api
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda m: json.dumps(m).encode('utf-8'))

    def on_data(self, data):
        try:
            newdata = datasend(data)
            self.producer.send('twitter', newdata)
            return True
        except KeyError:
            return True

    def on_error(self, status):
        if status == 420:
            return False

if __name__ == "__main__":

    #TWITTER API CONFIGURATIONS
    consumer_key = twitter_config.consumer_key
    consumer_secret = twitter_config.consumer_secret
    access_token = twitter_config.access_token
    access_secret = twitter_config.access_secret

    #TWITTER API AUTH
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)

    stream = tweepy.Stream(auth, listener=TwitterListener(api))
    WORDS_TO_TRACK = ["Bernie","Kamala","Biden"]
    while True:
        try:
            stream.filter(languages=["en"], track=WORDS_TO_TRACK)
        except:
            pass
