package org.example.TruckAssignment;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.TruckAssignment.Object.TruckCoordinate;
import org.example.TruckAssignment.Object.TruckCoordinateSerializer;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", TruckCoordinateSerializer.class.getName());


        KafkaProducer<String, TruckCoordinate> producer = new KafkaProducer<String, TruckCoordinate>(props);

        TruckCoordinate truckCoordinate = new TruckCoordinate();
        truckCoordinate.setId("truck-001");
        truckCoordinate.setLatitude("22.5726 N");
        truckCoordinate.setLongitude("88.3639 E");
        ProducerRecord<String, TruckCoordinate> record = new ProducerRecord<>("TruckTopic",truckCoordinate.getId() ,truckCoordinate);

        try{
            producer.send(record);
        }catch (Exception e)
        {
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }
}
