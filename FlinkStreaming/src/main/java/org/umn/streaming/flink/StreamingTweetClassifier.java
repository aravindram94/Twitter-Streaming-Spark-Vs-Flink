package org.umn.streaming.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.util.Properties;


public class StreamingTweetClassifier {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-streaming-tweet-consumer-group");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer010<>("twitter", new SimpleStringSchema(), properties));

        DataStream<String> wordCounts = stream
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String tweet_str) throws Exception {
                        JSONObject json = new JSONObject(tweet_str);
                        Integer sentiment = SentimentAnalyzer.findSentiment(json.getString("text"));
                        return json.put("sentiment", sentiment).toString();
                    }
                });

        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(
                "localhost:9092",            // broker list
                "flink-tweets-sentiments",                  // target topic
                new SimpleStringSchema());
        myProducer.setWriteTimestampToKafka(true);

        wordCounts.addSink(myProducer);

        env.execute("Streaming Tweets Classifier");

    }
}
