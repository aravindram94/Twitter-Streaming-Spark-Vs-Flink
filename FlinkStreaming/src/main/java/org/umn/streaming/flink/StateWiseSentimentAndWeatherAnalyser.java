package org.umn.streaming.flink;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.util.Properties;

public class StateWiseSentimentAndWeatherAnalyser {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-state-weather-consumer-group");
        DataStream<String> tweet_stream = env
                .addSource(new FlinkKafkaConsumer010<>("twittertweets", new SimpleStringSchema(), properties));
        DataStream<String> weather_stream = env
                .addSource(new FlinkKafkaConsumer010<>("weather", new SimpleStringSchema(), properties));
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        SlidingEventTimeWindows windowSpec = SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(2));

        DataStream<Tuple2<String, Double>> tweet_result = tweet_stream
                .map(tweet_str -> {
                    JSONObject json = new JSONObject(tweet_str);
                    Integer sentiment = SentimentAnalyzer.findSentiment(json.getString("text"));
                    String state_short_code = USStateMapper.getStateShortCode(json.getString("place_full_name"));
                    return new Tuple3<>(state_short_code, sentiment, 1);
                })
                .returns( TupleTypeInfo.getBasicTupleTypeInfo( String.class, Integer.class, Integer.class ) )
                .keyBy(0)
                .window(windowSpec)
                .apply (new WindowFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>, Tuple, TimeWindow>() {
                    public void apply (Tuple tuple,
                                       TimeWindow window,
                                       Iterable<Tuple3<String, Integer, Integer>> values,
                                       Collector<Tuple2<String, Double>> out) throws Exception {
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

                        out.collect(new Tuple2<>(place, sentiment));
                    }
                });

        DataStream<Tuple2<String, Double>> weather_result = weather_stream
                .map(weather_str -> {
                    JSONObject json = new JSONObject(weather_str);
                    String state_short_code = USStateMapper.getStateShortCode(json.getString("state"));
                    return new Tuple3<>(state_short_code, json.getInt("temperature"), 1);
                })
                .returns( TupleTypeInfo.getBasicTupleTypeInfo( String.class, Integer.class, Integer.class ) )
                .keyBy(0)
                .window(windowSpec)
                .apply (new WindowFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>, Tuple, TimeWindow>() {
                    public void apply (Tuple tuple,
                                       TimeWindow window,
                                       Iterable<Tuple3<String, Integer, Integer>> values,
                                       Collector<Tuple2<String, Double>> out) throws Exception {
                        int temperature_sum = 0;
                        int count = 0;
                        String place = "";
                        for (Tuple3 t: values) {
                            place = t.f0.toString();
                            temperature_sum += Integer.parseInt(t.f1.toString());
                            count += Integer.parseInt(t.f2.toString());
                        }
                        Double avg_temperature = 1.0 / (1.0 + Math.exp(-2* temperature_sum / count));
                        out.collect(new Tuple2<>(place, avg_temperature));
                    }
                });

        DataStream<String> result_stream = tweet_result.join(weather_result)
                .where(new KeySelector<Tuple2<String,Double>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Double> value) throws Exception {
                        return value.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple2<String,Double>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Double> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(windowSpec)
                .apply(new JoinFunction<Tuple2<String,Double>, Tuple2<String,Double>, Tuple3<String,Double, Double>>() {
                    @Override
                    public Tuple3<String, Double, Double> join(Tuple2<String, Double> sentiment,
                                                               Tuple2<String, Double> weather)
                            throws Exception {
                        return new Tuple3<>(sentiment.f0, sentiment.f1, weather.f1);
                    }
                })
                .map(new MapFunction<Tuple3<String, Double, Double>, String>() {
                    @Override
                    public String map(Tuple3<String, Double, Double> value) throws Exception {
                        return new JSONObject()
                                .put("place", value.f0)
                                .put("sentiment", value.f1)
                                .put("temperature", value.f2)
                                .toString();
                    }
                });


        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(
                "localhost:9092",            // broker list
                "flink-states-sentiments-weather",                  // target topic
                new SimpleStringSchema());
        myProducer.setWriteTimestampToKafka(true);

        result_stream.addSink(myProducer);

        env.execute("Streaming WordCount Example");

    }
}
