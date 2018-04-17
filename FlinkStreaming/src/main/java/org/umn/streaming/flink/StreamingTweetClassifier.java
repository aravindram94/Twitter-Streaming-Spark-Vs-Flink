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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.util.Properties;


public class StreamingTweetClassifier {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-wc-consumer-group");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer09<>("tweets", new SimpleStringSchema(), properties));
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        SlidingEventTimeWindows windowSpec = SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5));
        DataStream<String> wordCounts = stream
                .map(new MapFunction<String, Tweet>() {
                    @Override
                    public Tweet map(String tweet_str) throws Exception {
                        JSONObject json = new JSONObject(tweet_str);
                        return new Tweet(json.getString("id"), json.getString("text"), json.getString("place_full_name"));
                    }
                })
                .flatMap(( Tweet value, Collector<Tuple3<String, Integer, Integer>> out ) -> {
                    Integer sentiment = SentimentAnalyzer.findSentiment(value.text);
                    out.collect(new Tuple3<>(value.place, sentiment, 1));
                })
                .returns( TupleTypeInfo.getBasicTupleTypeInfo( String.class, Integer.class, Integer.class ) )
                .keyBy(0)
                .window(windowSpec)
                .apply (new WindowFunction<Tuple3<String, Integer, Integer>, String, Tuple, TimeWindow>() {
                    public void apply (Tuple tuple,
                                       TimeWindow window,
                                       Iterable<Tuple3<String, Integer, Integer>> values,
                                       Collector<String> out) throws Exception {
                        Long time = window.maxTimestamp();
                        int senti_sum = 0;
                        int count = 0;
                        String place = "";
                        for (Tuple3 t: values) {
                            place = t.f0.toString();
                            senti_sum = senti_sum + Integer.parseInt(t.f1.toString());
                            count = count + Integer.parseInt(t.f2.toString());
                        }
                        Double sentiment = 1.0 / (1.0 + Math.exp(-2* senti_sum / count));

                        out.collect (new JSONObject()
                                .put("time", time)
                                .put("place", place)
                                .put("sentiment", sentiment)
                                .put("count", count).toString());
                    }
                });
//                .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
//                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> v1,
//                                                        Tuple3<String, Integer, Integer> v2) {
//                        return new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2);
//                    }
//                }).map(new MapFunction<Tuple3<String, Integer, Integer>, String>() {
//                    @Override
//                    public String map(Tuple3<String, Integer, Integer> tuple) throws Exception {
//                        Double sentiment = 1.0 / (1.0 + Math.exp(-2* tuple.f1.doubleValue() / tuple.f2));
//                        return new JSONObject()
//                                .put("place", tuple.f0.toString())
//                                .put("sentiment", sentiment)
//                                .put("count", tuple.f2).toString();
////                        return new Tuple3<>(tuple.f0.toString(), sentiment, tuple.f2);
//                    }
//                });

        wordCounts.print();

        FlinkKafkaProducer09<String> myProducer = new FlinkKafkaProducer09<String>(
                "localhost:9092",            // broker list
                "states-sentiments",                  // target topic
                new SimpleStringSchema());

        wordCounts.addSink(myProducer);

        env.execute("Streaming WordCount Example");

    }
}
