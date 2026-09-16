package org.example.TruckAssignment.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());

        props.setProperty("schema.registry.url","http://localhost:8081");


        KafkaProducer<String, TruckCoordinates> producer = new KafkaProducer<String, TruckCoordinates>(props);

        TruckCoordinates truckCoordinate = new TruckCoordinates("truck-001","22.5726 N","88.3639 E");
        ProducerRecord<String, TruckCoordinates> record = new ProducerRecord<>("TruckTopic",truckCoordinate.getId().toString() ,truckCoordinate);

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
