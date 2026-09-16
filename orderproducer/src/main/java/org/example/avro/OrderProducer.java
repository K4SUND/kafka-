package org.example.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.avro.resources.Order;


import java.util.Properties;

public class OrderProducer {


    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");

        // Avro serializer
        props.setProperty("key.serializer",KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());

        // Schema Registry
        // This push the schema in to schema registry
        // Add schemaId in to record
        props.setProperty("schema.registry.url","http://localhost:8081");


        // Order message
        KafkaProducer<String, Order> producer = new KafkaProducer<String, Order>(props);

        Order order = new Order("Kasun","Laptop",4);
        ProducerRecord<String, Order> record = new ProducerRecord<>("OrderCSTopic", order.getCustomerName().toString(), order);

        try {
            producer.send(record);

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }
    }
}
