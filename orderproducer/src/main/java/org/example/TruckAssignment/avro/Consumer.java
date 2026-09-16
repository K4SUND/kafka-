package org.example.TruckAssignment.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
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
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("group.id", "Trucks");

        //schema registry
        props.setProperty("schema.registry.url", "http://localhost:8081");

        // specific -- TruckCoordinates
        props.setProperty("specific.avro.reader", "true");


        KafkaConsumer<String, TruckCoordinates> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("TruckTopic"));

        try {
            while (true) {
                ConsumerRecords<String, TruckCoordinates> truckPlaces = consumer.poll(Duration.ofSeconds(20));
                for (ConsumerRecord<String, TruckCoordinates> order : truckPlaces) {

                    System.out.println("Truck Id" + order.key());
                    TruckCoordinates truckCoordinate = order.value();
                    System.out.println("Latitude,Longitude respectively: " + truckCoordinate.getLatitude() + truckCoordinate.getLongitude());
                }

            }
        } finally {
            consumer.close();
        }


    }


}
