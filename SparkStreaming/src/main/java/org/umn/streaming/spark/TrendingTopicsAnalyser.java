package org.umn.streaming.spark;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.JSONObject;
import scala.Tuple2;

import java.util.*;

public class TrendingTopicsAnalyser {
    public static void main(String args[]) throws InterruptedException {
        String brokers = args[0];
        String topics = args[1];
        final int BATCH_INTERVAL = 100; // milliseconds
        final int WINDOW_SIZE = 4;
        final int SLIDING_FREQ = 2;
        final int TOP_N = 5;

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkTrendingTopics");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(BATCH_INTERVAL));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,"spark_kafka_tweets_classifier");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> result = messages.map(ConsumerRecord::value)
                .flatMap(tweet_str -> Arrays.asList(tweet_str.split(" ")).iterator())
                .filter(word -> word.length() > 0 && word.startsWith("#"))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(WINDOW_SIZE), Durations.seconds(SLIDING_FREQ))
                .mapToPair(Tuple2::swap)
                .transformToPair(word_count -> word_count.sortByKey(false))
                .transform(rdd ->  jssc.sparkContext().parallelize(rdd.take(TOP_N)))
                .map(word_count -> new JSONObject()
                        .put("word", word_count._2)
                        .put("count", word_count._1)
                        .toString());


        result.foreachRDD(rdd -> {
            rdd.foreachPartition(partitionOfRecords -> {
                Producer<Integer, String> producer = MyKafkaProducer.getProducer();
                while (partitionOfRecords.hasNext()) {
                    producer.send(new ProducerRecord<>("spark-trending-topics", 1, partitionOfRecords.next()));
                }

            });
        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
