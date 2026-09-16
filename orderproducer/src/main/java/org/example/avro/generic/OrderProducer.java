package org.example.avro.generic;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;



import java.util.Properties;

public class OrderProducer {


    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");

        // Avro serializer
        props.setProperty("key.serializer",KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());

        // Schema Registry
        // This push the schema in to schema registry
        // Add schemaId in to record
        props.setProperty("schema.registry.url","http://localhost:8081");


        // Order message
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(props);

        //Generic
        Parser parser = new Schema.Parser();
        Schema schema = parser.parse("""
                        {
                        "namespace": "org.example.avro.resources",
                        "type": "record",
                        "name": "Order",
                        "fields": [
                {"name": "customerName","type": "string"},
                {"name": "product","type": "string"},
                {"name": "quantity","type": "int"}
          ]
        }
        """);

        GenericRecord order = new GenericData.Record(schema);
        order.put("customerName","Kasun");
        order.put("product","Laptop");
        order.put("quantity",4);


        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("OrderCSTopic", order.get("customerName").toString(), order);

        try {
            producer.send(record);

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }
    }
}
