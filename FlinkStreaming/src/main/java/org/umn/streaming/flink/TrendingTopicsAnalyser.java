package org.umn.streaming.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.json.JSONObject;


import java.util.*;

public class TrendingTopicsAnalyser {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-trending-topics-consumer-group");

        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer010<>("twitterstream", new SimpleStringSchema(), properties));
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // Every 2 seconds, most trending of last 2 second of tweets.
        SlidingEventTimeWindows windowSpec = SlidingEventTimeWindows.of(Time.seconds(4), Time.seconds(2));

        DataStream<String> trending_topics = stream
                .flatMap(( String tweet_str, Collector<Tuple3<String, String, Integer>> out ) -> {
                    JSONObject json = new JSONObject(tweet_str);
                    // normalize and split the line into words
                    String id = json.getString("id");
                    String[] tokens = json.getString("text").toLowerCase().split( "\\s+|," );
                    Arrays.stream(tokens)
                            .filter(t -> t.length() > 0)
                            .filter(t -> t.startsWith("#"))
                            .forEach(t -> out.collect(new Tuple3<>(id, t, 1)));
                })
                .returns( TupleTypeInfo.getBasicTupleTypeInfo( String.class, String.class, Integer.class ) )
                .windowAll(windowSpec)
                .apply (new AllWindowFunction<Tuple3<String, String,Integer>, String, TimeWindow>() {

                    public void apply (TimeWindow window,
                                       Iterable<Tuple3<String, String, Integer>> values,
                                       Collector<String> out) throws Exception {
                        window.maxTimestamp();
                        HashMap<String, Integer> map = new HashMap<>();
                        for (Tuple3 t: values) {
                            map.put(t.f1.toString(), map.getOrDefault(t.f1.toString(), 0) + 1);
                        }

                        PriorityQueue<Word> queue = new PriorityQueue<>((a, b) -> b.count - a.count);
                        for(String key : map.keySet()) {
                            queue.add(new Word(key, map.get(key)));
                        }
                        int counter = 0;
                        while(queue.size() != 0 && counter < 5) {
                            Word word = queue.poll();
                            out.collect(new JSONObject()
                                    .put("word", word.text)
                                    .put("count", new Long(word.count))
                                    .toString());
                            counter++;
                        }
                    }
                });

//        wordCounts.print();
        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(
                "localhost:9092",            // broker list
                "flink-trending-topics",                  // target topic
                new SimpleStringSchema());

        myProducer.setWriteTimestampToKafka(true);

        trending_topics.addSink(myProducer);

        env.execute("Streaming WordCount Example");
    }
}

