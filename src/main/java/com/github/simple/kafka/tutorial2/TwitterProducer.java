package com.github.simple.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    //Tokens to access Twitter.
    String consumerKey = "Nea0520kQ1ScAZCkk1WWaRb6T";
    String consumerSecret = "74uX4kWOzq1Eu7wt0NzveqDRv1oV7Qm7WUsd0wW3YapiHtfm6s";
    String token = "106767306-aX1WAL4nfVE5kI619oVFpVwOq06iH64yNnHXIo8k";
    String secret = "alerjXGpeSTZZ5GuiLAqQPerVJRMZP3VVwAnawkVlGVtb";

    List<String> terms = Lists.newArrayList("bitcoin", "corona", "politics");


    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream.
         * This is where the client is going to put the messages.
         * */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);


        // Create a twitter client
        Client client= createTwitterClient(msgQueue);
        client.connect();

        // Create a Kafka Producer
        KafkaProducer<String,String> producer = createKafkaProducer();

        // Add a Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Application....");
            logger.info("Stopping Down client from twitter....");

            client.stop();
            logger.info("Closing producer....");
            producer.close(); // We need to close it to make sure it send all the messages stored in the memory to the Kafka Server before shutting down.
            logger.info("Done!!....");

        }));

        // Loop to send tweet to Kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try{
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch(InterruptedException e){
                e.printStackTrace();
                client.stop();
            }
            if(msg !=null){
                logger.info(msg);

                // We need to create this topic first.
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.error("Something bad happened "+ e);
                        }
                    }
                });
            }
        }
        logger.info("End of Application: ");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        /*
            Setup is described here : https://github.com/twitter/hbc
            Please read the document to understand how the Setup works.

         */

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey,
                                                 consumerSecret,
                                                 token,
                                                 secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;

    }

    public KafkaProducer<String, String> createKafkaProducer(){

        String bootstrapServer = "127.0.0.1:9092";

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // High throughput producer (at the expence of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024)); // 32 KB batch size


        // Create a safe Poducer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString((Integer.MAX_VALUE)));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");  // Can keep this a 5 otherwise use 1.
        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;

    }
}
