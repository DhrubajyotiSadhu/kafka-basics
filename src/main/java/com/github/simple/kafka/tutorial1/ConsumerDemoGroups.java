package com.github.simple.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);

        String bootstrapServers = "127.0.0.1:9092";
        String groupID = "my_fifth_application";
        String topic = "first_topic";

        // Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Converting the bytes written by the Producer to String. That is why we use Deserializer.
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Converting the bytes written by the Producer to String. That is why we use Deserializer.
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //(latest, none)

        // Create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe to our topics
        consumer.subscribe(Collections.singleton("first_topic"));
        // If you want multiple topics then use Arrays.asList("first_topic","second_topic",....)

        // Poll for the new data, as consumer will not get any gata until it asks for it.
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String,String> record: records){
                logger.info("Key: "+ record.key()+ " Value: "+ record.value());
                logger.info("Partition: "+ record.partition()+ " Offsets: "+ record.offset());
            }
        }
    }
}
