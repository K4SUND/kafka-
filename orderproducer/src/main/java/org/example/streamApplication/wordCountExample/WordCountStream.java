package org.example.streamApplication.wordCountExample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountStream {

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

        // for k table
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,0);



        // logic
        // stream topology
        StreamsBuilder builder = new StreamsBuilder();
        // read from a topic
        KStream<String, String> stream = builder.stream("wordCount-input");

        //flatMap  --- create a record for each element in  the list
        //groupBY --- group by Value
        KGroupedStream<String, String> groupedStream = stream.flatMapValues(value -> Arrays.asList(value.split(" "))).groupBy(((key, value) -> value));
        KTable<String, Long> countsTable = groupedStream.count();

        //convert to a stream
        // here output data type is <String,Long> , so we have to tell to stream
        // override default serializer
        countsTable.toStream().to("wordCount-output", Produced.with(Serdes.String(),Serdes.Long()));


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
