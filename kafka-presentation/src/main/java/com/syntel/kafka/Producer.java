package com.syntel.kafka;

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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class Producer {
    public static void main(String[] args) {
        String bootStrapServer = "127.0.0.1:9092";
        String topic = "presentation-topic";

        Properties producerProperties = createProducerProperties(bootStrapServer);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(producerProperties);

        kafkaProducer.send(new ProducerRecord<>(topic, "Hello world August"));

//        runBasicProducer(kafkaProducer, topic);

        runTwitterProducer(kafkaProducer, topic);

        kafkaProducer.close();
    }

    public static void runBasicProducer(KafkaProducer<String, String> kafkaProducer, String topic){
        Stream<String> messagesForProducer = Stream.iterate(0, x -> x + 1).map(x -> "Hello World " + x).limit(20);

        Stream<ProducerRecord<String, String>> producerRecords = messagesForProducer.map(x -> messageToRecord(topic, x));

        producerRecords.forEach(kafkaProducer::send);
    }

    public static void runTwitterProducer(KafkaProducer<String, String> kafkaProducer, String topic){
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        Client twitterClient = createTwitterClient(msgQueue);

        twitterClient.connect();

        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }

            if (msg != null) {
                System.out.println(msg);
                kafkaProducer.send(messageToRecord(topic, msg));
            }

        }

    }

    public static ProducerRecord<String, String> messageToRecord(String topic, String message){
        return new ProducerRecord<String, String>(topic, message);
    }

    private static Properties createProducerProperties(String bootStrapServer){
        Properties properties = new Properties();

//        returnProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
//        returnProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        returnProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        return properties;
    }


    public static Client createTwitterClient(BlockingQueue<String> msgQueue){
        String consumerKey = "Ob0Ag4MfhXbj5GIckWNduTylQ";
        String consumerSecret = "rgdVBMj4AztNhaYMl50F9dW2UkAlQfmdAKQQNuq9K8Kt6ZdX7J";
        String token = "1119711867809619968-Vfox7ov67mh1dH3jDdg24WrUWSlV25";
        String tokenSecret = "WVySqfUKy77ALZcYq9xq8PQYimrwqhEUafB6jLH78ukmL";

        List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");

        Hosts presentationHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint presentationEndpoint = new StatusesFilterEndpoint();

        presentationEndpoint.trackTerms(terms);

        Authentication twitterAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(presentationHosts)
                .authentication(twitterAuth)
                .endpoint(presentationEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client twitterClient = builder.build();
        return twitterClient;
    }

}
