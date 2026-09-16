package org.example.TruckAssignment.Object;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class TruckCoordinateDeserializer implements Deserializer<TruckCoordinate> {
    @Override
    public TruckCoordinate deserialize(String topic, byte[] data) {

        ObjectMapper objectMapper = new ObjectMapper();
        TruckCoordinate truckCoordinate = null;

        try {
            truckCoordinate = objectMapper.readValue(data,TruckCoordinate.class);
        } catch (IOException e) {
            e.printStackTrace();
        }


        return truckCoordinate;
    }
}
