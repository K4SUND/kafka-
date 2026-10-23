package org.example.propertiesConfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.POJO.Order;
import org.example.async.OrderCallback;


import java.util.Properties;

public class ProducerWithConfigClass {

    public static void main(String[] args) {

        Properties props = new Properties();

        // Instead of hardcoding, it uses ProducerConfig class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.example.POJO.customSerializerAndDeserializer.OrderSerializer");

        // transaction ID -- producer Id
        // -- should be different for producer instances
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"order-producer-1");
        // init Transaction, send -- blocking , so timeout config
//        props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG,"1000");





        KafkaProducer<String, Order> producer = new KafkaProducer<String, Order>(props);

        // order1
        Order orderOne = new Order();
        orderOne.setCustomerName("Kasun");
        orderOne.setProduct("Laptop");
        orderOne.setQuantity(4);

        // order2
        Order orderTwo= new Order();
        orderTwo.setCustomerName("Thimal");
        orderTwo.setProduct("Mobile");
        orderTwo.setQuantity(3);

        // init transaction
        producer.initTransactions();

        ProducerRecord<String, Order> recordOne = new ProducerRecord<>("OrderCSTopic", orderOne.getCustomerName(), orderOne);
        ProducerRecord<String, Order> recordTwo = new ProducerRecord<>("OrderCSTopic", orderTwo.getCustomerName(), orderTwo);

        try {

            // transaction --
            producer.beginTransaction();

            // not need callback -- because of commitTransaction
            producer.send(recordOne,new OrderCallback());
            producer.send(recordTwo);
            producer.commitTransaction();  // send exception if failed

        } catch (Exception e) {

            //abort transaction
            producer.abortTransaction();
            e.printStackTrace();

        } finally {
            producer.close();
        }
    }
}
