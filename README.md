#  twitter-bigdata-sentiment-analysis

Objective:
Developing a big data application to apply sentiment analysis on twitter data. reply according to the sentiment and to produce reports and visualize insights.

> Make sure to check out and follow remarks in (Environment Setup.txt) file for scripts to run correctly.

in the project files you will find 2 scripts. One is the scraper named (**tweets_publisher.py**) it collects data from twitter API and and publish it to a kafka topic named "tweet_user_topic". The other script named (**tweets_consumer_analyzer_replyer**) runs the spark job that reads stream from the mentioned kafka topic apply the required processing, replies to the tweets, and saves the tweets data to a DF on hdfs into parquet format.

## to create Kafka topic
cd /usr/hdp/current/kafka-broker/bin
/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitter_user_topic

## to submit spark job 
#make sure to include neccessary packages and jars in your spark-submit command#
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.6,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 --master local[*] /root/tweets_consumer_analyzer_replyer.py

# to build a hive table on top of this data
### creat external staging table
create external table twitter.tweet_user_stg(user_id string,user_name string ,user_screen_name string ,user_location string ,followers_count string ,friends_count string ,statuses_count string ,tweet_timestamp string,text string ,retweet_count string ,tweet_id string, tweet_date string, tweet_time string, polarity string, subjectivity string, sentiment string) stored as parquet location '< path to directory of data created by spark job>';

### create internal partitioned tweet_user_stg table paritoned by date
create table twitter.tweet_user_part(user_id string,user_name string ,user_screen_name string ,user_location string ,followers_count string ,friends_count string ,statuses_count string ,tweet_timestamp string,text string ,retweet_count string ,tweet_id string, tweet_time string, polarity string, subjectivity string, sentiment string) partitioned by (tweet_date string)stored as parquet;

### to insert data from staging table into partitioned table
From tweet_user_stg as stg
Insert overwrite table tweet_user_part partition(tweet_date)
Select
stg.user_id ,stg.user_name,stg.user_screen_name,stg.user_location,stg.followers_count, stg.friends_count,stg.statuses_count,stg.tweet_timestamp,stg.retweet_count,stg.tweet_id,stg.text, stg.tweet_date, stg.tweet_time, stg.polarity, stg.subjectivity , stg.sentiment 

### Incremental load into analytical table
From tweet_user_stg as stg
Insert append table tweet_user_part partition(tweet_date)
Select
stg.user_id ,stg.user_name,stg.user_screen_name,stg.user_location,stg.followers_count, stg.friends_count,stg.statuses_count,stg.tweet_timestamp,stg.retweet_count,stg.tweet_id,stg.text, stg.tweet_date, stg.tweet_time, stg.polarity, stg.subjectivity , stg.sentiment 
