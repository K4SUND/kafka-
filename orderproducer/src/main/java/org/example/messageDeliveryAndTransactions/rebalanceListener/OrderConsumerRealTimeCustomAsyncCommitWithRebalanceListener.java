package org.example.messageDeliveryAndTransactions.rebalanceListener;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.POJO.Order;
import org.example.POJO.customSerializerAndDeserializer.OrderDeserializer;

import java.time.Duration;
import java.util.*;

public class OrderConsumerRealTimeCustomAsyncCommitWithRebalanceListener {

    public static void main(String[] args) {

        //properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", OrderDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");
        props.setProperty("auto.commit.offset", "false");
        props.setProperty("auto.commit.interval.ms", "2000"); // 2s


        // this has the offset information
        // this is updating in the below processing logic
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        //consumer object
        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);

        // here it has the class
        class RebalanceHandler implements ConsumerRebalanceListener {


            // invoked when rebalance is triggered
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

                // here it gives the current offset argument in to commitSync method
                consumer.commitSync(currentOffsets);
            }

            // invoked when partitions are assigned
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        }

        // here in the subscribe method, it has overload version
        consumer.subscribe(Collections.singletonList("OrderCSTopic"), new RebalanceHandler());


        // infinitely
        try {
            while (true) {
                ConsumerRecords<String, Order> orders = consumer.poll(Duration.ofSeconds(20));
                int count = 0;

                for (ConsumerRecord<String, Order> order : orders) {

                    String customerName = order.key();
                    Order record = order.value();
                    System.out.println("Customer Name: " + customerName);
                    System.out.println("Product: " + record.getProduct());
                    System.out.println("Quantity: " + record.getQuantity());

                    // process and put in to hashmap
                    // use this for rebalance triggered
                    currentOffsets.put(new TopicPartition(order.topic(), order.partition()), new OffsetAndMetadata(order.offset() + 1));


                    // every 10
                    if (count % 10 == 0) {
//                        Map<TopicPartition, OffsetAndMetadata> currentOffsets = Collections.singletonMap(new TopicPartition(order.topic(), order.partition()), new OffsetAndMetadata(order.offset() + 1));
//                        currentOffsets.put(new TopicPartition(order.topic(), order.partition()), new OffsetAndMetadata(order.offset() + 1));
                        consumer.commitAsync(currentOffsets, new OffsetCommitCallback() {
                            @Override
                            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                                if (exception != null) {
                                    System.out.println("Commit is failed in offset " + offsets);
                                }
                            }
                        });

                    }

                    count++;
                }


            }

        } finally {
            consumer.close();
        }


    }
}
