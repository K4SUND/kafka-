package org.example.simpleConsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.POJO.Order;
import org.example.POJO.customSerializerAndDeserializer.OrderDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


// not assign to a group
// this has only need properties ( remove group id property and others )
public class OrderSimpleConsumer {

    public static void main(String[] args) {


        //properties
        Properties props = new Properties();

        // instead of hardcoding properties, it is using config class
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderDeserializer.class.getName());

        // standalone consumer
        // where it starts from --- begin or after this run
        // begin
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        //consumer object
        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);


        // here subscribe is not correct. We have to use assign method( assign partitiions to consumer )
        //subscription , assign
//        consumer.subscribe(Collections.singletonList("OrderCSTopic"));
        ArrayList<TopicPartition> partitions = new ArrayList();
        // add partitions related to topics manually
        partitions.add(new TopicPartition("MyTopic",0));  //parition 1
        partitions.add(new TopicPartition("MyTopic",1));  //partition 2

        consumer.assign(partitions);

        /*
        // if assign all the partitions
        // get how many partitions are there
        List<PartitionInfo> partitionInfos = consumer.partitionsFor("MyTopic");
        for(PartitionInfo partitionInfo:partitionInfos)
        {
            // add all
            partitions.add(new TopicPartition("MyTopic",partitionInfo.partition()));

        }

        consumer.assign(partitions);


         */

        //polling  ( get records related to assigned/subscribed topic, partitions )
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
