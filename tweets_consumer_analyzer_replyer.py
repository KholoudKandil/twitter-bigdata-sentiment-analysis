from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
from textblob import TextBlob
from datetime import datetime
import tweepy as tw
import time

def reply_to_tweet(tweet): 
    user = tweet.user_screen_name
    if tweet.polarity>=0:
      msg = "@%s Thank you for spreading positivity! Keep it up!" %user
      print("################Postivie Reply Posted##########################################")
    else:
      msg = "@%s We're sorry you're feeling that way! Tell us how can we help!" %user
      print("################Negative Reply Posted##########################################")
    msg_sent = api.update_status(msg, tweet.tweet_id)
    
    
def preprocessing(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words.printSchema()
    
    tweetdf= words.withColumn('user_id',f.split('word',';').getItem(0))\
    .withColumn('user_name', f.split('word', ';').getItem(1))\
    .withColumn('user_screen_name', f.split('word', ';').getItem(2))\
    .withColumn('user_location', f.split('word', ';').getItem(3))\
    .withColumn('followers_count', f.split('word', ';').getItem(4))\
    .withColumn('friends_count', f.split('word', ';').getItem(5))\
    .withColumn('statuses_count', f.split('word', ';').getItem(6))\
    .withColumn('tweet_timestamp', f.split('word', ';').getItem(7))\
    .withColumn('text', f.split('word', ';').getItem(8))\
    .withColumn('retweet_count', f.split('word', ';').getItem(9))\
    .withColumn('tweet_id', f.split('word', ';').getItem(10))
    tweetdf = tweetdf.withColumn('tweet_date', f.split(tweetdf['tweet_timestamp'], ' ')[0] ).withColumn('tweet_time', f.split(tweetdf['tweet_timestamp'], ' ')[1] )
    tweetdf = tweetdf.dropDuplicates(['tweet_id'])
    tweetdf.printSchema()
    return tweetdf

# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity
def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity
def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, FloatType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, FloatType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words
if __name__ == "__main__":
    #preparing for reply
    consumer_key = "insert yours"
    consumer_secret = "insert yours"
    access_key = "insert yours"
    access_secret = "insert yours"
    
    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tw.API(auth)
    # create Spark session
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()
    spark.conf.set("spark.sql.parquet.compression.codec", "gzip")

    
    #### Subscribe to 1 topic
    topic_name = "tweet_user_topic"
    lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "172.18.0.2:6667").option("subscribe", topic_name).load()
    lines.printSchema()
    # Preprocess the data
    tweetdf = preprocessing(lines)
    #words.show()
    # text classification to define polarity and subjectivity
    tweetdf = text_classification(tweetdf)
    tweetdf = tweetdf.withColumn('sentiment',when( tweetdf.polarity >= 0, "positive").otherwise("negative"))
    
    tweetdf = tweetdf.repartition(1)   
    
    reply_query = tweetdf.writeStream.foreach(reply_to_tweet).start()
    
    query = tweetdf.writeStream.queryName("all_tweets")\
        .outputMode("append").format("parquet")\
        .option("path", "hdfs://sandbox-hdp.hortonworks.com:8020/user/kholoud/twitter/tweet_user_stg")\
        .option("checkpointLocation","hdfs://sandbox-hdp.hortonworks.com:8020/user/kholoud/twitter/tweet_user_checkpoint")\
        .trigger(processingTime='50 seconds').start()
    query.awaitTermination()
    reply_query.awaitTermination()
