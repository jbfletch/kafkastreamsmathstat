import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import serde.JsonSerde;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaStreamsTumblingSATDemo {
    private final Serde<JsonNode> jsonSerde = new JsonSerde();
    public static void main(String[] args) {
        final KafkaStreamsTumblingSATDemo kafkaStreamsTumblingSATDemo = new KafkaStreamsTumblingSATDemo();
        KafkaStreams streams = new KafkaStreams(kafkaStreamsTumblingSATDemo.getTopology(),kafkaStreamsTumblingSATDemo.getKafkaProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
    Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-tumbling-sat-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, jsonSerde.getClass().getName());
        return props;
    }
    Topology getTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        final Serdes.ListSerde<Integer> listSerde = new Serdes.ListSerde<>(ArrayList.class,Serdes.Integer());

        KStream<String, JsonNode> baseStream = builder.stream("incoming-observations-topic");


        TimeWindows tumblingWindow =
            TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10));
        KTable<Windowed<String>, List<Integer>> observationsByClass = baseStream
            .groupBy((k,v) -> v.path("sneaker").asText())
            .windowedBy(tumblingWindow)
            .aggregate(() -> new ArrayList<Integer>(),
                (key, value, aggregate) -> {
                    aggregate.add(value.path("peeps").asInt());
                    return aggregate;
                }
                , Materialized.<String, List<Integer>, WindowStore<Bytes,
                    byte[]>>as("classes").withValueSerde(listSerde))
            ;
        observationsByClass
            .toStream()
            .filterNot((k, v) -> v == null)
            .filter((k,v) -> v.size() == 30)
            .map(
                ((stringWindowed, integers) -> {
                    DescriptiveStatistics stats= new DescriptiveStatistics();
                    double[] array = integers.stream().mapToDouble(Integer::doubleValue).toArray();
                    Arrays.stream(array)
                        .forEach(obs -> {
                            stats.addValue(obs);
                        });

                    System.out.println("Number of Values: " + String.valueOf(stats.getN()));
                    System.out.println("Min Value: " + String.valueOf(stats.getMin()));
                    System.out.println("Max Value: " + String.valueOf(stats.getMax()));
                    System.out.println("Mean: " + String.valueOf(stats.getMean()));
                    System.out.println("Skewness Function: " + String.valueOf(stats.getSkewnessImpl()));
                    System.out.println("Skewness: " + String.valueOf(stats.getSkewness()));
                    System.out.println("Kurtosis: " + String.valueOf(stats.getKurtosis()));

                    return KeyValue.pair(stringWindowed, integers);
                })
            ) .print(Printed.<Windowed<String>, List<Integer>>toSysOut().withLabel("Full Key"));
        observationsByClass
            .toStream()
            .filterNot((k, v) -> v == null)
            .filter((k,v) -> v.size() == 30)
            .map(
                ((stringWindowed, integers) -> {
                    DescriptiveStatistics stats= new DescriptiveStatistics();
                    double[] array = integers.stream().mapToDouble(Integer::doubleValue).toArray();
                    Arrays.stream(array)
                        .forEach(obs -> {
                            stats.addValue(obs);
                        });
                    System.out.println(String.valueOf(stats.getMax()));
                    System.out.println(String.valueOf(stats.getMean()));
                    System.out.println(String.valueOf(stats.getSkewness()));

                    return KeyValue.pair(stringWindowed, integers);
                })
            )
            .to("updates-by-class",Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class),listSerde));

        return builder.build();

    }
}
