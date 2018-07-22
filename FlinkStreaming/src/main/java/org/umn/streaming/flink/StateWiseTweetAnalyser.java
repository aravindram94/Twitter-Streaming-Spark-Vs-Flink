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

public class StateWiseTweetAnalyser {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-statewise-consumer-group");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer010<>("tweet", new SimpleStringSchema(), properties));
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        SlidingEventTimeWindows windowSpec = SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(5));
        DataStream<String> wordCounts = stream
                .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(String tweet_str) throws Exception {
                        JSONObject json = new JSONObject(tweet_str);
                        Integer sentiment = SentimentAnalyzer.findSentiment(json.getString("text"));
                        String state_short_code = USStateMapper.getStateShortCode(json.getString("place_full_name"));
                        return new Tuple3<>(state_short_code, sentiment, 1);
                    }
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

        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(
                "localhost:9092",            // broker list
                "flink-states-sentiments",                  // target topic
                new SimpleStringSchema());
        myProducer.setWriteTimestampToKafka(true);

        wordCounts.addSink(myProducer);

        env.execute("Streaming WordCount Example");

    }
}
