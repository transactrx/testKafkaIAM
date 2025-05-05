package com.example.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

// Add AWS region provider
//import com.amazonaws.regions.Regions;

public class KafkaProducerApp {
    private static final String TOPIC_NAME = "test_iam";
    private static final String BOOTSTRAP_SERVERS = 
        "boot-3om.iam.powerlinedevkafka.u02dwn.c3.kafka.us-east-1.amazonaws.com:14098," +
        "boot-7xl.iam.powerlinedevkafka.u02dwn.c3.kafka.us-east-1.amazonaws.com:14100," +
        "boot-yv7.iam.powerlinedevkafka.u02dwn.c3.kafka.us-east-1.amazonaws.com:14099";

    public static void main(String[] args) {
        // Create producer properties
        Properties props = new Properties();
        
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Sets up TLS for encryption and SASL for authN.
        props.put("security.protocol", "SASL_SSL");
        
        // Identifies the SASL mechanism to use.
        props.put("sasl.mechanism", "OAUTHBEARER");
        
        // Binds SASL client implementation.
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        
        // Encapsulates constructing a SigV4 signature based on extracted credentials.
        // The SASL client bound by "sasl.jaas.config" invokes this class.
        props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMOAuthBearerLoginCallbackHandler");
        props.put("sasl.login.callback.handler.class", "software.amazon.msk.auth.iam.IAMOAuthBearerLoginCallbackHandler");
        
        // Add debugging settings for Kafka client
        props.put("debug", "auth,security");
        
        // Add connection timeouts
        props.put("socket.connection.setup.timeout.ms", "10000");
        props.put("connections.max.idle.ms", "60000");
        
        // Add SSL configs - special handling for AWS MSK IAM authentication
//        props.put("ssl.protocol", "TLSv1.2");
//        props.put("ssl.enabled.protocols", "TLSv1.2");
//        props.put("ssl.endpoint.identification.algorithm", "https");
//
//        // Use the default Java truststore for Amazon trust
//        props.put("ssl.truststore.type", "JKS");
        
        // Set AWS region explicitly
        System.setProperty("aws.region", "us-east-1");
        
//        // Enable debug for auth issues
//        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
//        System.setProperty("software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider.SDK_DEFAULT_CREDENTIALS_PROVIDER_CHAIN_DEBUG", "true");
//
//        // Enable additional AWS debug logging
//        System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka", "DEBUG");
//        System.setProperty("org.slf4j.simpleLogger.log.software.amazon.msk", "DEBUG");
        
        // Print out AWS creds info to verify (without secrets)
//        try {
//            com.amazonaws.auth.AWSCredentialsProvider provider = new com.amazonaws.auth.DefaultAWSCredentialsProviderChain();
//            com.amazonaws.auth.AWSCredentials credentials = provider.getCredentials();
//            System.out.println("AWS credentials loaded successfully: " + credentials.getAWSAccessKeyId().substring(0, 5) + "...");
//        } catch (Exception e) {
//            System.err.println("Error loading AWS credentials: " + e.getMessage());
//            e.printStackTrace();
//        }
        
        // Set much shorter client timeout values for quicker failure detection
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "100");
        props.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "1000");
        props.put(ProducerConfig.RETRIES_CONFIG, "2");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "3000");
        props.put("socket.connection.setup.timeout.ms", "3000");
        props.put("socket.connection.setup.timeout.max.ms", "5000");

//        try (AdminClient adminClient = AdminClient.create(props)) {
//            Optional<Integer> replicas=Optional.of(1);
//            Optional<Short> repFactor=Optional.of(Short.parseShort("1"));
//            NewTopic newTopic = new NewTopic(TOPIC_NAME, replicas,repFactor);
//            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
//
//            // The createTopics() method returns a future, so we need to wait for it to complete
//            result.all().get();
//            System.out.println("Topic '" + TOPIC_NAME + "' created successfully.");
//
//        } catch (InterruptedException | ExecutionException e) {
//            System.err.println("Error creating topic: " + e.getMessage());
//            System.exit(1);
//        }


        try (org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = 
                new org.apache.kafka.clients.producer.KafkaProducer<>(props)) {

            // Send 5 test messages
            for (int i = 1; i <= 50000; i++) {
                String key = "key-" + i;
                String value = "test message " + i;
                
                System.out.println("Sending: (" + key + ", " + value + ")");
                
                ProducerRecord<String, String> record = 
                    new ProducerRecord<>(TOPIC_NAME, key, value);
                
                // Send record and get metadata (blocking call)
                try {
                    producer.send(record).get();
//                    System.out.println("Message " + i + " sent successfully");
                } catch (InterruptedException | ExecutionException e) {
                    System.err.println("Error sending message " + i);
                    e.printStackTrace();
                }
            }
            
            System.out.println("All messages sent successfully!");
        }
    }
}