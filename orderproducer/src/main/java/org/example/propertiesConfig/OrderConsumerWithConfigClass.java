package org.example.propertiesConfig;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.POJO.Order;
import org.example.POJO.customSerializerAndDeserializer.OrderDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;



public class OrderConsumerWithConfigClass {

    public static void main(String[] args) {


        //properties
        Properties props = new Properties();

        // instead of hardcoding properties, it is using config class
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OrderGroup");

        // new properties
        // minimum bytes before consume
        props.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,"1024");
        // maximum wait before consume
        props.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,"200");
        // consumer should be send heat beat to coordinator every 1000ms interval
        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,"1000");
        // consumer should be send heartbeat before 3000ms , otherwise it considers as dead then rebalance triggered
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"3000");

        // per partition, maximum bytes --- 30 partitions, 5 consumers, 1 consumer has 6 partitions.
        props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,"1MB");
        //
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");  //from the begining of the partition
//        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");  //after consumer starts, from that offset (ignore previous ones)


        // give a unique id to consumer
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,"OrderConsumer");
        // maximum records can be returned by poll method
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");


        // partition assignor
//        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());  // round-robin
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());  // default --- range



        //consumer object
        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        //subscription
        consumer.subscribe(Collections.singletonList("OrderCSTopic"));
        //polling
        ConsumerRecords<String,Order> orders = consumer.poll(Duration.ofSeconds(20));
        for(ConsumerRecord<String,Order> order : orders)
        {
            String customerName = order.key();
            Order record = order.value();
            System.out.println("Customer Name: "+customerName);
            System.out.println("Product: "+record.getProduct());
            System.out.println("Quantity: "+record.getQuantity());

        }

        consumer.close();




    }
}
