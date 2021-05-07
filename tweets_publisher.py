from kafka import KafkaConsumer , KafkaProducer
from datetime import datetime
import tweepy as tw
import time

consumer_key = "oKHLc1OEtFqDmes3rzx7axFxH"
consumer_secret = "8uDv2Wxcp9GCFFbv7V9GwmOZ1PtFylEYGNplYIWDfejrLu0Gsj"
access_key = "1385696117938077700-baAP81Eor9jNoZ8sbwWKx7serPVntU"
access_secret = "lu4dUyb6O2yfhRFiX3dt9kKMTefAVT9q6vS7cbr1jhfMT"

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tw.API(auth)

def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))
    
producer = KafkaProducer(bootstrap_servers='172.18.0.2:6667')
topic_name = 'tweet_user_topic'

def get_twitter_data():
    res = api.search("Covid19")
    for i in res:
        print("##############################################################")
        record =''
        record +=str(i.user.id_str)
        record += ';'
        record +=str(i.user.name)
        record += ';'
        record +=str(i.user.screen_name)
        record += ';'
        record +=str(i.user.location)
        record += ';'
        record +=str(i.user.followers_count)
        record += ';'
        record +=str(i.user.friends_count)
        record += ';'
        record +=str(i.user.statuses_count)
        record += ';'
        record += str(normalize_timestamp(str(i.created_at)))
        record += ';'
        record +=str(i.text)
        record += ';'
        record +=str(i.retweet_count)
        record += ';'
        record +=str(i.id)
        record += ';'
        producer.send(topic_name, str.encode(record))
        print(record)
        print("##############################################################")
        
get_twitter_data()
def periodic_work(interval):
    while True:
        get_twitter_data()
        time.sleep(interval)
        
periodic_work(30* 0.1)
