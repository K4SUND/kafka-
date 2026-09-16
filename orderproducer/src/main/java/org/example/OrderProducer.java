package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.POJO.Order;
import org.example.async.OrderCallback;


import java.util.Properties;

public class OrderProducer {


    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        // Order message
        // Custom object -> custom serializer
        props.setProperty("value.serializer", "org.example.POJO.customSerializerAndDeserializer.OrderSerializer");

//        KafkaProducer<String, Integer> producer = new KafkaProducer<String, Integer>(props);
        // Order message
        KafkaProducer<String, Order> producer = new KafkaProducer<String, Order>(props);

        /*
         public ProducerRecord(String topic, V value) {
        this(topic, null, null, null, value, null);    }
         */


//        ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic", "Laptop", 10);

        Order order = new Order();
        order.setCustomerName("Kasun");
        order.setProduct("Laptop");
        order.setQuantity(4);
        ProducerRecord<String, Order> record = new ProducerRecord<>("OrderCSTopic", order.getCustomerName(), order);

        try {
            producer.send(record);

            /*
            async send

            producer.send(record,new OrderCallback());

             */

            /*
//            sync send
//            waiting for response
            RecordMetadata recordMetadata = producer.send(record).get();
            system.out.println(recordMetadata.partition());
            system.out.println(recordMetadata.offset());
            system.out.println("Message sent successfully");
             */

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }
    }
}
