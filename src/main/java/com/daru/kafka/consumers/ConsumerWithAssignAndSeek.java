package com.daru.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithAssignAndSeek {

    public static void main(String[] args) {

        Logger LOGGER = LoggerFactory.getLogger(ConsumerWithAssignAndSeek.class);
        String bootstrapServer =  "127.0.0.1:9092";
        String topic = "first_topic";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // earliest -  read from very start of topic
        // latest - read from only the new messages
        // none - throw error if no offsets being saved
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(properties);

        // assign and seek
        // mostly used to replay data
        // fetch a specific message\

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;
        boolean keepReading = true;

        // poll for new data
        // obv not good having while true but good enough for learning
        while (keepReading){
            ConsumerRecords<String, String> records
                    = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){
                LOGGER.info("Key: " + record.key() + ", Value: " + record.value());
                LOGGER.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                numberOfMessagesReadSoFar += 1;

                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepReading = false;
                    break;
                }

            }

            LOGGER.info("Existing the while loop");

        }

    }

}
