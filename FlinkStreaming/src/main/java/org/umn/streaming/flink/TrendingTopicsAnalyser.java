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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.util.Collector;
import org.json.JSONObject;


import java.util.*;

public class TrendingTopicsAnalyser {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-wc-consumer-group");
//        DataStream<String> stream = env.fromElements(
//                "{\"text\": \"Nah it was for church lmao https://t.co/sbMOsPFcbI\", \"place_type\": \"city\", \"place_name\": \"Waukegan\", \"place_full_name\": \"Waukegan, IL\"}",
//                "{\"text\": \"So true it very true\", \"place_type\": \"city\", \"place_name\": \"Burleson\", \"place_full_name\": \"Burleson, TX\"}");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer09<>("tweets", new SimpleStringSchema(), properties));
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // Every 2 seconds, most trending of last 2 second of tweets.
        SlidingEventTimeWindows windowSpec = SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2));

        DataStream<Tuple2<String, Long>> wordCounts = stream
                .map(new MapFunction<String, Tweet>() {
                    @Override
                    public Tweet map(String tweet_str) throws Exception {
                        JSONObject json = new JSONObject(tweet_str);
                        return new Tweet(json.getString("id"), json.getString("text"), json.getString("place_full_name"));
                    }
                })
                .flatMap(( Tweet value, Collector<Tuple3<String, String, Integer>> out ) -> {
                    // normalize and split the line into words
                    String id = value.id;
                    String[] tokens = value.text.toLowerCase().split( "\\s+|," );
                    Arrays.stream(tokens)
                            .filter(t -> t.length() > 0)
                            .filter(t -> t.startsWith("#"))
                            .forEach(t -> out.collect(new Tuple3<>(id, t, 1)));
                } )
                .returns( TupleTypeInfo.getBasicTupleTypeInfo( String.class, String.class, Integer.class ) )
                .windowAll(windowSpec)
                .apply (new AllWindowFunction<Tuple3<String, String,Integer>, Tuple2<String,Long>, TimeWindow>() {

                    public void apply (TimeWindow window,
                                       Iterable<Tuple3<String, String, Integer>> values,
                                       Collector<Tuple2<String,Long>> out) throws Exception {
                        window.maxTimestamp();
                        HashMap<String, Integer> map = new HashMap<>();
                        HashSet<String> tweet_ids = new HashSet<>();
                        for (Tuple3 t: values) {
                            map.put(t.f1.toString(), map.getOrDefault(t.f1.toString(), 0) + 1);
                            tweet_ids.add(t.f0.toString());
                        }

                        PriorityQueue<Word> queue = new PriorityQueue<>((a, b) -> b.count - a.count);
                        for(String key : map.keySet()) {
                            queue.add(new Word(key, map.get(key)));
                        }
                        int counter = 0;
                        while(queue.size() != 0 && counter < 5) {
                            Word word = queue.poll();
                            out.collect(new Tuple2<>(word.text, new Long(word.count)));
                            counter++;
                        }
                        out.collect(new Tuple2<>("total tweets count", new Long(tweet_ids.size())));
                        out.collect(new Tuple2<>("window_start", window.getStart()));
                        out.collect(new Tuple2<>("window_end", window.getEnd()));
                    }
                });

        wordCounts.print();

        env.execute("Streaming WordCount Example");
    }
}

