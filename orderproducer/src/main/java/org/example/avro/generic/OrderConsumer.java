package org.example.avro.generic;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;



import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    public static void main(String[] args) {


        //properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");

        // Avro deserializers
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer",KafkaAvroDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");
        props.setProperty("schema.registry.url","http://localhost:8081");

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("OrderCSTopic"));

        ConsumerRecords<String,GenericRecord> orders = consumer.poll(Duration.ofSeconds(20));
        for(ConsumerRecord<String,GenericRecord> order : orders)
        {
            String customerName = order.key();
            GenericRecord record = order.value();
            System.out.println("Customer Name: "+customerName);
            System.out.println("Product: "+record.get("product"));
            System.out.println("Quantity: "+record.get("quantity"));

        }

        consumer.close();

    }
}
