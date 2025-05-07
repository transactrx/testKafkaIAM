package com.example.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaProducerApp {
    private static final String TOPIC_NAME = "testingtesting";
    //private static final String BOOTSTRAP_SERVERS ="boot-ahs.scram.powerlinedevkafka.u02dwn.c3.kafka.us-east-1.amazonaws.com:14099,boot-8rg.scram.powerlinedevkafka.u02dwn.c3.kafka.us-east-1.amazonaws.com:14100,boot-rce.scram.powerlinedevkafka.u02dwn.c3.kafka.us-east-1.amazonaws.com:14098";
    private static final String BOOTSTRAP_SERVERS ="boot-6li.powerlinedevkafka.mqhv5a.c19.kafka.us-east-1.amazonaws.com:9096,boot-7xp.powerlinedevkafka.mqhv5a.c19.kafka.us-east-1.amazonaws.com:9096,boot-2f1.powerlinedevkafka.mqhv5a.c19.kafka.us-east-1.amazonaws.com:9096";
    private static final AtomicInteger MESSAGE_COUNTER = new AtomicInteger(0);
    private static final AtomicInteger SUCCESS_COUNTER = new AtomicInteger(0);
    private static final AtomicInteger ERROR_COUNTER = new AtomicInteger(0);

    public static void main(String[] args) {
        int numThreads = 32; // Default: match number of partitions for optimal parallelism
        int totalMessages = 50000; // Default total number of messages

        // Parse command line arguments if provided
        if (args.length >= 1) {
            try {
                numThreads = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid number of threads, using default: " + numThreads);
            }
        }
        
        if (args.length >= 2) {
            try {
                totalMessages = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid number of messages, using default: " + totalMessages);
            }
        }
        
        System.out.println("Using " + numThreads + " threads to send " + totalMessages + " messages");
        System.out.println("Command line arguments received: " + (args.length > 0 ? args.length : "none"));
        if (args.length > 0) {
            System.out.println("Args[0]: " + args[0]);
        }
        if (args.length > 1) {
            System.out.println("Args[1]: " + args[1]);
        }

        // Create producer properties
        Properties props = new Properties();
        
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Sets up TLS for encryption and SASL for authN.
        props.put("security.protocol", "SASL_SSL");
        
        // Identifies the SASL mechanism to use.
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        
        // Binds SASL client implementation.
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"melaraj\" password=\"MaherManuco99\";");
        
        // Encapsulates constructing a SigV4 signature based on extracted credentials.
        // The SASL client bound by "sasl.jaas.config" invokes this class.
        //props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        
        // Add debugging settings for Kafka client
        props.put("debug", "auth,security");
        
        // Set AWS region explicitly
        System.setProperty("aws.region", "us-east-1");
        
        // Performance-optimized producer configuration
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5"); // Wait to batch more records
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024); // 32KB batch size
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 128 * 1024 * 1024); // 128MB buffer
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // Efficient compression
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // Only wait for leader acknowledgment
        
        // More efficient error handling and retries for performance
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "50");
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "50");
        props.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "500");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // Multiple in-flight requests
        props.put("socket.connection.setup.timeout.ms", "3000");
        props.put("socket.connection.setup.timeout.max.ms", "5000");

        try (AdminClient adminClient = AdminClient.create(props)) {
            // Increased number of partitions for better parallelism and throughput
            Optional<Integer> partitions = Optional.of(32); // Using 32 partitions for high throughput
            Optional<Short> repFactor = Optional.of(Short.parseShort("1"));
            NewTopic newTopic = new NewTopic(TOPIC_NAME, partitions, repFactor);

            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));

            // The createTopics() method returns a future, so we need to wait for it to complete
            result.all().get();
            System.out.println("Topic '" + TOPIC_NAME + "' created successfully.");

        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error creating topic: " + e.getMessage());
        }

        // Start the multi-threaded message production
        long startTime = System.currentTimeMillis();
        sendMessagesMultiThreaded(props, numThreads, totalMessages);
        long endTime = System.currentTimeMillis();
        
        System.out.println("All messages sent: " + SUCCESS_COUNTER.get() + " successful, " + 
                          ERROR_COUNTER.get() + " failed");
        System.out.println("Time taken = " + ((endTime - startTime) / 1000) + " seconds");
    }
    
    private static void sendMessagesMultiThreaded(Properties props, int numThreads, int totalMessages) {
        // Calculate messages per thread
        int messagesPerThread = totalMessages / numThreads;
        int remainingMessages = totalMessages % numThreads;
        
        // Create thread pool
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        
        // Each thread gets its own producer for better parallelism
        System.out.println("Creating dedicated Kafka producers for each thread");
        
        // Create and submit producer tasks
        for (int i = 0; i < numThreads; i++) {
            // Last thread gets any remaining messages
            int threadMessages = messagesPerThread + (i == numThreads - 1 ? remainingMessages : 0);
            int threadId = i + 1;
            
            executor.submit(() -> {
                // Create dedicated producer for each thread
                try (KafkaProducer<String, String> threadProducer = new KafkaProducer<>(props)) {
                    try {
                        produceMessages(threadProducer, threadId, threadMessages);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
            
        // Wait for all threads to complete
        try {
            latch.await();
        } catch (InterruptedException e) {
            System.err.println("Thread interrupted while waiting for completion: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
        
        System.out.println("All threads completed with dedicated producers.");
    }
    
    private static void produceMessages(KafkaProducer<String, String> producer, int threadId, int messageCount) {
        System.out.println("Thread " + threadId + " starting to send " + messageCount + " messages");
        
        // Use thread ID to ensure keys are distributed across partitions
        String threadPrefix = "thread-" + threadId + "-";
        String value = "asd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsdasd1231asdasdas asdass asd ds sdsd";
        
        // Batch messages for faster sending (100 messages at a time)
        final int BATCH_SIZE = 100;
        for (int i = 0; i < messageCount; i += BATCH_SIZE) {
            int batchEnd = Math.min(i + BATCH_SIZE, messageCount);
            
            for (int j = i; j < batchEnd; j++) {
                int messageId = MESSAGE_COUNTER.incrementAndGet();
                // Include thread ID in key to ensure better partition distribution
                String key = threadPrefix + "key-" + messageId;
                
                // Use async send without waiting for each message
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        SUCCESS_COUNTER.incrementAndGet();
                    } else {
                        ERROR_COUNTER.incrementAndGet();
                        System.err.println("Thread " + threadId + " error sending message: " + exception.getMessage());
                    }
                });
            }
            
            // Print progress every batch
            System.out.println("Thread " + threadId + " progress: " + Math.min(i + BATCH_SIZE, messageCount) + "/" + messageCount);
        }
        
        // Ensure all messages are sent before returning
        producer.flush();
        System.out.println("Thread " + threadId + " completed sending " + messageCount + " messages");
    }
}