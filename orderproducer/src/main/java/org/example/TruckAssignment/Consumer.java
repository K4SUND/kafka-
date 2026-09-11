package org.example.TruckAssignment;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("group.id", "Trucks");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("TruckTopic"));

        ConsumerRecords<String,String> truckPlaces = consumer.poll(Duration.ofSeconds(20));
        for(ConsumerRecord<String,String > order : truckPlaces)
        {
            System.out.println("Truck Id"+ order.key());
            System.out.println("Latitude,Longitude respectively: "+order.value());
        }

        consumer.close();


    }


}
