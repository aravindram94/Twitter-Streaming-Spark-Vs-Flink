import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    spark = SparkSession.builder.appName("TestKakfa").getOrCreate()

    brokers, topic = sys.argv[1:]

    schema = StructType() \
        .add("id", StringType()) \
        .add("text", StringType()) \
        .add("user_id", StringType()) \
        .add("timestamp_ms", StringType()) \
        .add("place_full_name", StringType())


    data_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("subscribe", topic) \
        .load()

    kafka_df_string = data_stream.selectExpr("CAST(value AS STRING) as json")

    tweets_table = kafka_df_string.select(from_json("json", schema).alias("data"))

    query = tweets_table.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()



    # data_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    #
    # #streamed_tweets = kafkaStream.map(lambda x: json.loads(x[1]))
    #
    # streamed_tweets = kafkaStream.map(lambda x: x[1])
    #
    # streamed_tweets.count().map(lambda x: 'Tweets in this batch is %s' % x).pprint()
    #
    # streamed_tweets_words = streamed_tweets.flatMap(lambda line: line.split(" "))
    #
    # hashtags = streamed_tweets_words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
    #
    # hashtags_counts = hashtags.reduceByKey(lambda x, y: x + y)
    #
    # sorted_hashtags_counts = hashtags_counts.transform((lambda foo: foo.sortBy(lambda x: -x[1])))
    #
    # top_five_authors = sorted_hashtags_counts.transform \
    #     (lambda rdd: sc.parallelize(rdd.take(5)))
    # top_five_authors.pprint()
    #
    #
    # # streamed_tweets.count().map(lambda x: 'Tweets in this batch is %s' % x).pprint()
    # #
    # # authors = streamed_tweets.map(lambda tweet: tweet['user']['screen_name'])
    # #
    # # author_counts = authors.countByValue().pprint()
    #
    # # print(sorted_hashtags_counts)
    #
    #
    # ssc.start()
    # ssc.awaitTermination()