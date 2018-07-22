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

public class StateWiseTweetAnalyser {
    public static void main(String args[]) throws InterruptedException {
        String brokers = args[0];
        String topics = args[1];

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkStateWiseTweetsClassifier");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(100));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,"spark_kafka_tweets_classifier_new");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        JavaDStream<String> result = messages.map(ConsumerRecord::value)
                .mapToPair(tweet_str -> {
                    JSONObject json = new JSONObject(tweet_str);
                    Integer sentiment = SentimentAnalyzer.findSentiment(json.getString("text"));
                    String state_short_code = USStateMapper.getStateShortCode(json.getString("place_full_name"));
                    return new Tuple2<>(state_short_code, new Tuple2<>(sentiment, 1));
                })
                .reduceByKeyAndWindow((i1, i2) -> {
                    return new Tuple2<>(i1._1 + i2._2, i1._2 + i2._2);
                }, Durations.seconds(1), Durations.seconds(1))
                .map(x -> {
                    Double sentiment = 1.0 / (1.0 + Math.exp(-2* x._2._1 / x._2._2));
                    return new JSONObject()
                            .put("place", x._1)
                            .put("sentiment", sentiment)
                            .put("count", x._2._2).toString();
                });


        result.foreachRDD(rdd -> {
            rdd.foreachPartition(partitionOfRecords -> {

                Producer<Integer, String> producer = MyKafkaProducer.getProducer();
                while (partitionOfRecords.hasNext()) {
                    producer.send(new ProducerRecord<>("spark-states-sentiments", 1, partitionOfRecords.next()));
                }

            });
        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
