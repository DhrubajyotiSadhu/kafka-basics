package com.github.simple.kafka.tutorial1;

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

/*

    This program will help in reading from a particular offset from within a partition.

 */
public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";

        // Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Converting the bytes written by the Producer to String. That is why we use Deserializer.
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Converting the bytes written by the Producer to String. That is why we use Deserializer.
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //(latest, none)

        // Create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        // Assign and seek is mostly used to replay data or fetch a specific message

        // Assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offSetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // Seek
        consumer.seek(partitionToReadFrom, offSetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepReading = true;
        int numberOfMessagesReadSoFar = 0;

        // Poll for the new data, as consumer will not get any gata until it asks for it.
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String,String> record: records){
                numberOfMessagesReadSoFar += 1;
                logger.info("Key: "+ record.key()+ " Value: "+ record.value());
                logger.info("Partition: "+ record.partition()+ " Offsets: "+ record.offset());
                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepReading = false;  // exit while loop
                    break;
                }
            }
        }
    }
}
