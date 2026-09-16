package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.POJO.Order;
import org.example.POJO.customSerializerAndDeserializer.OrderDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumerRealTime {

    public static void main(String[] args) {


        //properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
//        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // same
        props.setProperty("key.deserializer", StringDeserializer.class.getName());


//        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

//      custom deserializer ( order )
        props.setProperty("value.deserializer", OrderDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");


        //consumer object
//        KafkaConsumer<String,Integer> consumer = new KafkaConsumer<>(props);
        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        //subscription
//        consumer.subscribe(Collections.singletonList("OrderTopic"));
        consumer.subscribe(Collections.singletonList("OrderCSTopic"));


        // infinitely
        try {
            while (true) {

                // start polling
                // after 20s again poll
                ConsumerRecords<String, Order> orders = consumer.poll(Duration.ofSeconds(20));


                for (ConsumerRecord<String, Order> order : orders) {

                    String customerName = order.key();
                    Order record = order.value();
                    System.out.println("Customer Name: " + customerName);
                    System.out.println("Product: " + record.getProduct());
                    System.out.println("Quantity: " + record.getQuantity());


                }
            }

        } finally {
            consumer.close();
        }


    }
}
