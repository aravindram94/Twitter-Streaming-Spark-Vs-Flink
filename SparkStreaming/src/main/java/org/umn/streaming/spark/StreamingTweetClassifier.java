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

import java.util.*;

public class StreamingTweetClassifier {
    public static void main(String args[]) throws InterruptedException {
        String brokers = args[0];
        String topics = args[1];
        final int BATCH_INTERVAL = 100; // milliseconds

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkTweetsClassifier");

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
                .map(tweet_str -> {
                    JSONObject json = new JSONObject(tweet_str);
                    Integer sentiment = SentimentAnalyzer.findSentiment(json.getString("text"));
                    return json.put("sentiment", sentiment).toString();
                });


        result.foreachRDD(rdd -> {
            rdd.foreachPartition(partitionOfRecords -> {
                Producer<Integer, String> producer = MyKafkaProducer.getProducer();
                while (partitionOfRecords.hasNext()) {
                    producer.send(new ProducerRecord<>("spark-tweets-sentiments", 1, partitionOfRecords.next()));
                }

            });
        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
