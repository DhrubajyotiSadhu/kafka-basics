package com.github.simple.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("Kafka World");
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);


        String bootstrapServer = "127.0.0.1:9092";

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "hello Rivu "+ Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            // Create a Producer Record
            ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic, key, value); // As the key go added, the same key will always go to the same partition.

            logger.info("\n Key: "+ key);  // Log the key. Observe the same key goes to the same partition.
            //Key: id_0   Partition: 1
            //Key: id_1   Partition: 1
            //Key: id_2   Partition: 0
            //Key: id_3   Partition: 0
            //Key: id_4   Partition: 0

            // Send Data - Asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes everytime a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("\nReceived new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else {
                        logger.error("Error while producing: ", e);
                    }

                }
            }).get();  // Block the .send() to make it synchronous - don't do it in production.
        }
            producer.flush();
            producer.close();

        }

}
