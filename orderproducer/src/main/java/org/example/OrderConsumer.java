package org.example;

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
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty("group.id", "OrderGroup");


        //consumer object
        KafkaConsumer<String,Integer> consumer = new KafkaConsumer<>(props);
        //subscription
        consumer.subscribe(Collections.singletonList("OrderTopic"));
        //polling
//        consumer.poll(Duration.ZERO);
//        consumer.poll(Duration.ofMillis(1000));
        ConsumerRecords<String,Integer> orders = consumer.poll(Duration.ofSeconds(20));
        for(ConsumerRecord<String,Integer> order : orders)
        {
            System.out.println("Product Name "+order.key());
            System.out.println("Quantity "+order.value());
        }

        consumer.close();

    }
}
