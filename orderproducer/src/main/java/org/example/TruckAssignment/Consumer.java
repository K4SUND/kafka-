package org.example.TruckAssignment;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.TruckAssignment.Object.TruckCoordinate;
import org.example.TruckAssignment.Object.TruckCoordinateDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", TruckCoordinateDeserializer.class.getName());
        props.setProperty("group.id", "Trucks");

        KafkaConsumer<String, TruckCoordinate> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("TruckTopic"));

        ConsumerRecords<String, TruckCoordinate> truckPlaces = consumer.poll(Duration.ofSeconds(20));
        for (ConsumerRecord<String, TruckCoordinate> order : truckPlaces) {


            System.out.println("Truck Id" + order.key());
            TruckCoordinate truckCoordinate = order.value();
            System.out.println("Latitude,Longitude respectively: " + truckCoordinate.getLatitude() + truckCoordinate.getLongitude());
        }

        consumer.close();


    }


}
