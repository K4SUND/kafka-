package org.example.streamApplication;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class DataFlowStream {

    public static void main(String[] args) {

        // properties
        Properties props = new Properties();
        // here it use put()
        // Id of the stream application ( unique )
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"stream-data-flow");

        //kafka server ( broker informations ), can add multiple brokers, use ',' .
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");

        // default serializer and deserializer
        // serializer
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // deserializer
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());



        // logic
        // stream topology
        StreamsBuilder builder = new StreamsBuilder();
        // read from a topic
        KStream<String, String> stream = builder.stream("stream-input");
        // transform
        stream.foreach(((key, value) -> System.out.println("Key:"+key+" Value:"+value)));

        //filter method
//        KStream<String, String> filteredStream = stream.filter((key, value) -> value.contains("token"));
        //map method after filtering
//        KStream<String, String> filteredStream = stream.filter((key, value) -> value.contains("token")).mapValues(value -> value.toUpperCase());
        //map method -- mapValue
        KStream<String, String> filteredStream = stream.filter((key, value) -> value.contains("token")).map((key, value) -> new KeyValue<>("Key",value.toUpperCase()));


        //write to output topic
//        stream.to("stream-output");
        //send filtered stream
        filteredStream.to("stream-output");
        // in one line
//        stream.filter((key, value) -> value.contains("token")).to("stream-output");


        // create the topology
        Topology topology = builder.build();
        //  description of the topology
        System.out.println(topology.describe());

        // start the stream
        KafkaStreams streams = new KafkaStreams(topology,props);
        streams.start();


        //access process
        //and stop
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }
}
