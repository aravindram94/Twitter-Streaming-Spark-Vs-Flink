from pyspark import SparkContext, SparkConf
import pyspark_cassandra
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys

def saveToCassandra(rdd, keyspace, table):
    pickledRDD = rdd._toPickleSerialization()
    rdd.ctx._jvm.CassandraJavaUtil.javaFunctions(pickledRDD._jrdd)\
        .saveToCassandra(keyspace, table)

if __name__ == "__main__":

    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1")

    sc = pyspark_cassandra.CassandraSparkContext(appName="TwitterStreamAnalyser", conf=conf)
    sc.setLogLevel("WARN")

    # print(sc.cassandraTable("test01", "countries").collect())
    #
    # rdd = sc.parallelize([{
    #     "id": 2,
    #     "official_name": "name1",
    #     "captital_city": "cap1"
    # },{
    #     "id": 3,
    #     "official_name": "name2",
    #     "captital_city": "cap2"
    # },])
    #
    # rdd.saveToCassandra(
    #     "test01",
    #     "countries"
    # )
    #
    # print(sc.cassandraTable("test01", "countries").collect())

    # sc.read \
    #     .format("org.apache.spark.sql.cassandra") \
    #     .options(table="kv", keyspace="test") \
    #     .load().show()

    ssc = StreamingContext(sc, 0.1)

    brokers, topic = sys.argv[1:]

    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    #streamed_tweets = kafkaStream.map(lambda x: json.loads(x[1]))

    streamed_tweets = kafkaStream.map(lambda x: x[1])

    streamed_tweets.count().map(lambda x: 'Tweets in this batch is %s' % x).pprint()

    streamed_tweets_words = streamed_tweets.flatMap(lambda line: line.split(" "))

    hashtags = streamed_tweets_words.filter(lambda w: '#' in w).map(lambda x: (x, 1))

    hashtags_counts = hashtags.reduceByKey(lambda x, y: x + y)

    sorted_hashtags_counts = hashtags_counts.transform((lambda foo: foo.sortBy(lambda x: -x[1])))

    top_five_authors = sorted_hashtags_counts.transform \
        (lambda rdd: sc.parallelize(rdd.take(5)))
    top_five_authors.pprint()


    # streamed_tweets.count().map(lambda x: 'Tweets in this batch is %s' % x).pprint()
    #
    # authors = streamed_tweets.map(lambda tweet: tweet['user']['screen_name'])
    #
    # author_counts = authors.countByValue().pprint()

    # print(sorted_hashtags_counts)


    ssc.start()
    ssc.awaitTermination()