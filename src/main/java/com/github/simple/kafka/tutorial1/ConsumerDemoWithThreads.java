package com.github.simple.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {

        new ConsumerDemoWithThreads().run();

    }

    private ConsumerDemoWithThreads(){

    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

        String bootstrapServers = "127.0.0.1:9092";
        String groupID = "my_sixth_application";
        String topic = "first_topic";

        // Latch for dealing with multiple threads.
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer");

        // Create the Consuer Runnable
        Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupID, topic, latch);

        // Starts the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // Add a shutdown hook to properly shut dwn the application
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught Shutdown Hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            }catch(InterruptedException e){
                logger.error("Application got interupted", e);
            }finally {
                logger.info("Application has exited");
            }
        }

        ));

        try {
            latch.await();
        }catch(InterruptedException e){
            logger.error("Application got interupted", e);
        }finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);


        public ConsumerRunnable(String bootstrapServers, String groupID, String topic, CountDownLatch latch) {
            this.latch = latch;
            // Properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Converting the bytes written by the Producer to String. That is why we use Deserializer.
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Converting the bytes written by the Producer to String. That is why we use Deserializer.
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //(latest, none)

            // Create a consumer
            consumer = new KafkaConsumer<String, String>(properties);


            // Subscribe to our topics
            consumer.subscribe(Collections.singleton("first_topic"));

        }

        @Override
        public void run() {
            try {
                // Poll for the new data, as consumer will not get any gata until it asks for it.
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + " Value: " + record.value());
                        logger.info("Partition: " + record.partition() + " Offsets: " + record.offset());
                    }
                }

            } catch(WakeupException e){
                logger.info(("\nReceived Shutdown Signal! \n"));
            } finally {
                consumer.close();

                // Tell our main code that we are done with our consumer.
                latch.countDown();
            }
        }

        public void shutdown() {
            // The wakeup method is a special method to interrupt consumer.poll()
            // It will throw an exception: WakeupException
            consumer.wakeup();

        }
    }
}
