package org.example.partitioner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.POJO.Order;
import org.example.POJO.customSerializerAndDeserializer.OrderDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerWithPartition {
    public static void main(String[] args) {


        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", OrderDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");



        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderCSTopic"));
        ConsumerRecords<String,Order> orders = consumer.poll(Duration.ofSeconds(20));
        for(ConsumerRecord<String,Order> order : orders)
        {
            String customerName = order.key();
            Order record = order.value();
            System.out.println("Customer Name: "+customerName);
            System.out.println("Product: "+record.getProduct());
            System.out.println("Quantity: "+record.getQuantity());

            // display partition information
            System.out.println("Partition: "+order.partition());

        }

        consumer.close();

    }

}
