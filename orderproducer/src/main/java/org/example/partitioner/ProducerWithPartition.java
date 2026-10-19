package org.example.partitioner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.POJO.Order;

import java.util.Properties;

public class ProducerWithPartition {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.example.POJO.customSerializerAndDeserializer.OrderSerializer");
        // partition property
        props.setProperty("partitioner.class",CustomPartitioner.class.getName());



        KafkaProducer<String, Order> producer = new KafkaProducer<String, Order>(props);
        Order order = new Order();
        order.setCustomerName("Kasun");
        order.setProduct("Laptop");
        order.setQuantity(4);
        ProducerRecord<String, Order> record = new ProducerRecord<>("OrderCSTopic", order.getCustomerName(), order);

        try {
            producer.send(record);

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }
    }

}
