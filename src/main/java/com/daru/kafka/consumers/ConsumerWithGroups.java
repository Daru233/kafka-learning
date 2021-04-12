package com.daru.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithGroups {

    public static void main(String[] args) {

        Logger LOGGER = LoggerFactory.getLogger(ConsumerWithGroups.class);
        String bootstrapServer =  "127.0.0.1:9092";
        // change groupID to "refresh app"
        String groupID = "first_app";
        String topic = "first_topic";


        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        // earliest -  read from very start of topic
        // latest - read from only the new messages
        // none - throw error if no offsets being saved
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(properties);

        // subscribe consumer to topic
        // collection.singleton is sub to only one topic
        // consumer.subscribe(Collections.singleton("first_topic"));
        consumer.subscribe(Arrays.asList(topic));


        // poll for new data
        // obv not good having while true but good enough for learning
        while (true){
            ConsumerRecords<String, String> records
                    = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){
                LOGGER.info("Key: " + record.key() + ", Value: " + record.value());
                LOGGER.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }

    }

}
