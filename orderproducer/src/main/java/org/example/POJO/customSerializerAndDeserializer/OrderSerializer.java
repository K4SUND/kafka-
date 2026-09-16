package org.example.POJO.customSerializerAndDeserializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.example.POJO.Order;

public class OrderSerializer implements Serializer<Order> {

    @Override
    public byte[] serialize(String topic, Order data) {
        byte[] response = null;
        ObjectMapper objectMapper = new ObjectMapper();

        //  Object->String->Bytes
        try {
            response = objectMapper.writeValueAsString(data).getBytes();
        } catch (JsonProcessingException e) {
           e.printStackTrace();
        }

        return response;
    }
}
