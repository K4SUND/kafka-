package org.example.streamApplication;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class AssignmentFive {

    public static void main(String[] args) {

        // properties
        Properties props = new Properties();
        // Each Streams application must have its own ID. The ID also identifies
        // the consumer group and therefore its saved input offsets.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"assignment-five-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");



        // logic
        // stream topology
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(
                "stream-input-3",
                Consumed.with(Serdes.String(), Serdes.String())
        );
        stream.foreach((key, value) -> System.out.println("Key:" + key + " Value:" + value));

        // The console producer sends text, so parse it for the calculation and
        // convert the result back to text before the String serializer writes it.
        stream.filter((key, value) -> isInteger(value))
                .mapValues(value -> Integer.parseInt(value.trim()))
                .filter((key, value) -> value % 2 == 0)
                .mapValues(value -> Integer.toString(value * 3))
                .to("stream-output-3", Produced.with(Serdes.String(), Serdes.String()));

        // create the topology
        Topology topology = builder.build();
        System.out.println(topology.describe());

        // start the stream
        KafkaStreams streams = new KafkaStreams(topology,props);
        streams.setStateListener((newState, oldState) ->
                System.out.println("Kafka Streams state: " + oldState + " -> " + newState));
        streams.start();


        //access process
        //and stop
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }

    private static boolean isInteger(String value) {
        if (value == null) {
            return false;
        }

        try {
            Integer.parseInt(value.trim());
            return true;
        } catch (NumberFormatException exception) {
            System.err.println("Ignoring non-numeric value: " + value);
            return false;
        }
    }
}
