package org.example.TruckAssignment.partition;

import io.confluent.common.utils.Utils;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.example.TruckAssignment.Object.TruckCoordinate;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
        TruckCoordinate truckCoordinate  = (TruckCoordinate) value;
        // custom logic
        if(truckCoordinate.getLatitude().equals("37.2431") && truckCoordinate.getLongitude().equals("115.793")){
            return 5;
        }

        // divide total number of partitions
        return (Math.abs(Utils.murmur2(keyBytes))%partitions.size()-1);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
