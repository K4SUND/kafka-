package org.example.TruckAssignment.Object;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class TruckCoordinateSerializer implements Serializer<TruckCoordinate> {
    @Override
    public byte[] serialize(String topic, TruckCoordinate data) {

        byte[] response = null;
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            response = objectMapper.writeValueAsString(data).getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }


        return response;
    }
}
