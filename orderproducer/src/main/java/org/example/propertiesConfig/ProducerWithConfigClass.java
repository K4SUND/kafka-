package org.example.propertiesConfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.POJO.Order;


import java.util.Properties;

public class ProducerWithConfigClass {

    public static void main(String[] args) {

        Properties props = new Properties();

        // Instead of hardcoding, it uses ProducerConfig class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.example.POJO.customSerializerAndDeserializer.OrderSerializer");


        //
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // 0,1,all
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,"343434") ; // default ->256MB
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"gzip");  //gzip, snappy,lz4
        props.setProperty(ProducerConfig.RETRIES_CONFIG,"2");
        props.setProperty(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG,"400");



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
