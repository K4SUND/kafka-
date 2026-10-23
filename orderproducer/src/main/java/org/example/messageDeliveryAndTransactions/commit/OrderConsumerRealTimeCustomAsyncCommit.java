package org.example.messageDeliveryAndTransactions.commit;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.POJO.Order;
import org.example.POJO.customSerializerAndDeserializer.OrderDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class OrderConsumerRealTimeCustomAsyncCommit {

    public static void main(String[] args) {


        //properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
//      custom deserializer ( order )
        props.setProperty("value.deserializer", OrderDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");


        //default --- autocommit -- enable
//        props.setProperty("enable.auto.commit","false");  // disabling
        // disable auto commiting offset after interval (sync )
        props.setProperty("auto.commit.offset", "false");
        //default time = 5s (but this is drived by polling method below
        props.setProperty("auto.commit.interval.ms", "2000"); // 2s


        //consumer object
        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderCSTopic"));


        // infinitely
        try {
            while (true) {

                // start polling
                // after 20s again poll
                ConsumerRecords<String, Order> orders = consumer.poll(Duration.ofSeconds(20));


                // custom commit when every 10 records
                int count = 0;

                for (ConsumerRecord<String, Order> order : orders) {

                    String customerName = order.key();
                    Order record = order.value();
                    System.out.println("Customer Name: " + customerName);
                    System.out.println("Product: " + record.getProduct());
                    System.out.println("Quantity: " + record.getQuantity());


                    if(count%10==0)
                    {

                        // overloaded methods
                        // list, topic, partition, offset

                        // after finishes the process, commit the offset
                        // Async  ---
                        // retry -- is problematic
                        // order may be change -- then rebalancing results duplication processing

                        // here  offset + 1 ----> track correct offset
                        consumer.commitAsync(Collections.singletonMap(new TopicPartition(order.topic(),order.partition()),new OffsetAndMetadata(order.offset()+1)),new OffsetCommitCallback() {
                            @Override
                            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                                if (exception != null) {
                                    System.out.println("Commit is failed in offset " + offsets);
                                }
                            }
                        });

                    }

                    count ++;
                }


            }

        } finally {
            consumer.close();
        }


    }
}
