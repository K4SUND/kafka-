package org.example.simpleConsumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.POJO.Order;
import org.example.async.OrderCallback;


import java.util.Properties;

public class ProducerToTest {

    public static void main(String[] args) {

        Properties props = new Properties();

        // Instead of hardcoding, it uses ProducerConfig class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.example.POJO.customSerializerAndDeserializer.OrderSerializer");




        KafkaProducer<String, Order> producer = new KafkaProducer<String, Order>(props);

        // order1
        Order orderOne = new Order();
        orderOne.setCustomerName("Kasun");
        orderOne.setProduct("Laptop");
        orderOne.setQuantity(4);


        ProducerRecord<String, Order> recordOne = new ProducerRecord<>("MyTopic", orderOne.getCustomerName(), orderOne);

        try {
            producer.send(recordOne,new OrderCallback());

        } catch (Exception e) {

            e.printStackTrace();

        } finally {
            producer.close();
        }
    }
}
