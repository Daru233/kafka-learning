package com.daru.kafka.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {

    public static void main(String[] args) {

        Logger LOGGER = LoggerFactory.getLogger(ProducerWithKeys.class);

        String bootstrapServer =  "127.0.0.1:9092";

        // producer propoerties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        // Key String, Value String
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);




        // just to send a bunch of data
        for (int i = 0; i < 50; i++) {

            String topic = "first_topic";
            String value = "hello_world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            // create producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            // check logs,
            // by providing a key,
            // it is GUARANTEED that the same key ALWAYS goes to the same partition
            LOGGER.info("Key: " + key);

            // send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or exception is thrown
                    if (e == null){
                        // record was sent successfully
                        LOGGER.info("Received new metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        LOGGER.error("Error while producing: ", e);
                    }

                }
            });

        }



        // flush data
        producer.flush();
        // flush and close producer
        producer.close();

    }

}
