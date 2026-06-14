package me.matthew.flink.backpacktfforward.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for managing Kafka configuration from environment variables.
 * Handles reading and validation of Kafka connection parameters and consumer properties.
 */
@Slf4j
public class KafkaConfiguration {

    // Required environment variables
    private static final String KAFKA_BROKERS_ENV = "KAFKA_BROKERS";
    private static final String KAFKA_TOPIC_ENV = "KAFKA_TOPIC";
    private static final String KAFKA_CONSUMER_GROUP_ENV = "KAFKA_CONSUMER_GROUP";
    
    // Optional environment variables
    private static final String KAFKA_START_TIMESTAMP_ENV = "KAFKA_START_TIMESTAMP";
    private static final String KAFKA_START_TIMESTAMP_MINUTES_ENV = "KAFKA_START_TIMESTAMP_MINUTES";
    private static final int DEFAULT_KAFKA_TIMESTAMP_MINUTES = 30;
    
    // Prefix for additional Kafka consumer properties
    private static final String KAFKA_CONSUMER_PREFIX = "KAFKA_CONSUMER_";

    /**
     * Reads Kafka broker addresses from environment variable.
     * @return Comma-separated list of Kafka broker addresses
     * @throws IllegalArgumentException if KAFKA_BROKERS is not set or empty
     */
    public static String getKafkaBrokers() {
        String brokers = System.getenv(KAFKA_BROKERS_ENV);
        if (brokers == null || brokers.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s is required but not set or empty", KAFKA_BROKERS_ENV)
            );
        }
        return brokers.trim();
    }

    /**
     * Reads Kafka topic name from environment variable.
     * @return Kafka topic name
     * @throws IllegalArgumentException if KAFKA_TOPIC is not set or empty
     */
    public static String getKafkaTopic() {
        String topic = System.getenv(KAFKA_TOPIC_ENV);
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s is required but not set or empty", KAFKA_TOPIC_ENV)
            );
        }
        return topic.trim();
    }

    /**
     * Reads Kafka consumer group ID from environment variable.
     * @return Kafka consumer group ID
     * @throws IllegalArgumentException if KAFKA_CONSUMER_GROUP is not set or empty
     */
    public static String getConsumerGroup() {
        String consumerGroup = System.getenv(KAFKA_CONSUMER_GROUP_ENV);
        if (consumerGroup == null || consumerGroup.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s is required but not set or empty", KAFKA_CONSUMER_GROUP_ENV)
            );
        }
        return consumerGroup.trim();
    }

    /**
     * Reads Kafka start timestamp from environment variable if set.
     * @return Optional timestamp in milliseconds, or null if not set
     * @throws IllegalArgumentException if KAFKA_START_TIMESTAMP is set but not a valid number
     */
    public static Long getStartTimestamp() {
        String timestampStr = System.getenv(KAFKA_START_TIMESTAMP_ENV);
        if (timestampStr != null && !timestampStr.trim().isEmpty()) {
            try {
                return Long.parseLong(timestampStr.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    String.format("Environment variable %s must be a valid timestamp in milliseconds, got: %s",
                        KAFKA_START_TIMESTAMP_ENV, timestampStr), e
                );
            }
        }

        int minutes = DEFAULT_KAFKA_TIMESTAMP_MINUTES;
        String minutesStr = System.getenv(KAFKA_START_TIMESTAMP_MINUTES_ENV);
        if (minutesStr != null && !minutesStr.trim().isEmpty()) {
            try {
                minutes = Integer.parseInt(minutesStr.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    String.format("Environment variable %s must be a valid integer, got: %s",
                        KAFKA_START_TIMESTAMP_MINUTES_ENV, minutesStr), e
                );
            }
        }

        return System.currentTimeMillis() - Duration.ofMinutes(minutes).toMillis();
    }

    /**
     * Reads additional Kafka consumer properties from environment variables with KAFKA_CONSUMER_ prefix.
     * Environment variables like KAFKA_CONSUMER_AUTO_OFFSET_RESET become consumer property auto.offset.reset.
     * @return Properties object containing additional Kafka consumer configuration
     */
    public static Properties getKafkaConsumerProperties() {
        Properties properties = new Properties();
        
        System.getenv().forEach((key, value) -> {
            if (key.startsWith(KAFKA_CONSUMER_PREFIX) && key.length() > KAFKA_CONSUMER_PREFIX.length()) {
                // Extract the property name after the prefix
                String propertyName = key.substring(KAFKA_CONSUMER_PREFIX.length());
                
                // Convert from UPPER_CASE to lower.case format
                String kafkaPropertyName = propertyName.toLowerCase().replace('_', '.');
                
                properties.setProperty(kafkaPropertyName, value);
                log.debug("Added Kafka consumer property: {} = {}", kafkaPropertyName, value);
            }
        });
        
        return properties;
    }

    /**
     * Validates that all required Kafka configuration is present and valid.
     * Also validates broker connectivity and topic existence.
     * @throws IllegalArgumentException if any required configuration is missing or invalid
     * @throws KafkaException if broker connectivity or topic validation fails
     */
    public static void validateConfiguration() {
        log.info("Validating Kafka configuration...");
        
        try {
            // Validate required environment variables by calling getters
            String brokers = getKafkaBrokers();
            String topic = getKafkaTopic();
            String consumerGroup = getConsumerGroup();
            
            log.info("Kafka configuration validated successfully:");
            log.info("  Brokers: {}", brokers);
            log.info("  Topic: {}", topic);
            log.info("  Consumer Group: {}", consumerGroup);
            
            // Log additional consumer properties if any
            Properties additionalProps = getKafkaConsumerProperties();
            if (!additionalProps.isEmpty()) {
                log.info("  Additional consumer properties: {}", additionalProps.size());
                additionalProps.forEach((key, value) -> 
                    log.debug("    {} = {}", key, value)
                );
            }
            
            // Validate broker connectivity and topic existence
            validateBrokerConnectivity(brokers);
            validateTopicExists(brokers, topic);
            
        } catch (IllegalArgumentException e) {
            log.error("Kafka configuration validation failed: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Kafka connectivity validation failed: {}", e.getMessage());
            throw new KafkaException("Failed to validate Kafka connectivity", e);
        }
    }

    /**
     * Validates that the Kafka brokers are reachable and responsive.
     * @param brokers Comma-separated list of broker addresses
     * @throws KafkaException if brokers are not reachable within timeout
     */
    private static void validateBrokerConnectivity(String brokers) {
        log.info("Validating broker connectivity to: {}", brokers);
        
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", brokers);
        adminProps.put("request.timeout.ms", "10000"); // 10 second timeout
        adminProps.put("connections.max.idle.ms", "5000"); // 5 second idle timeout
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            // Try to get cluster metadata to verify connectivity
            var clusterMetadata = adminClient.describeCluster();
            var clusterId = clusterMetadata.clusterId().get(10, TimeUnit.SECONDS);
            log.info("Successfully connected to Kafka cluster: {}", clusterId);
            
        } catch (TimeoutException e) {
            String errorMsg = String.format("Timeout connecting to Kafka brokers: %s. " +
                "Please verify broker addresses and network connectivity.", brokers);
            log.error(errorMsg);
            throw new KafkaException(errorMsg, e);
            
        } catch (ExecutionException e) {
            String errorMsg = String.format("Failed to connect to Kafka brokers: %s. " +
                "Error: %s", brokers, e.getCause().getMessage());
            log.error(errorMsg);
            throw new KafkaException(errorMsg, e.getCause());
            
        } catch (Exception e) {
            String errorMsg = String.format("Unexpected error connecting to Kafka brokers: %s", brokers);
            log.error(errorMsg, e);
            throw new KafkaException(errorMsg, e);
        }
    }

    /**
     * Validates that the specified Kafka topic exists and is accessible.
     * @param brokers Comma-separated list of broker addresses
     * @param topic Topic name to validate
     * @throws KafkaException if topic does not exist or is not accessible
     */
    private static void validateTopicExists(String brokers, String topic) {
        log.info("Validating topic existence: {}", topic);
        
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", brokers);
        adminProps.put("request.timeout.ms", "10000"); // 10 second timeout
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            DescribeTopicsResult topicsResult = adminClient.describeTopics(Collections.singletonList(topic));
            TopicDescription topicDescription = topicsResult.values().get(topic).get(10, TimeUnit.SECONDS);
            
            int partitionCount = topicDescription.partitions().size();
            log.info("Topic '{}' exists with {} partitions", topic, partitionCount);
            
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                String errorMsg = String.format("Kafka topic '%s' does not exist. " +
                    "Please create the topic before starting the application.", topic);
                log.error(errorMsg);
                throw new KafkaException(errorMsg, e.getCause());
            } else {
                String errorMsg = String.format("Failed to validate topic '%s': %s", topic, e.getCause().getMessage());
                log.error(errorMsg);
                throw new KafkaException(errorMsg, e.getCause());
            }
            
        } catch (TimeoutException e) {
            String errorMsg = String.format("Timeout validating topic '%s'. " +
                "Please verify topic exists and broker connectivity.", topic);
            log.error(errorMsg);
            throw new KafkaException(errorMsg, e);
            
        } catch (Exception e) {
            String errorMsg = String.format("Unexpected error validating topic '%s'", topic);
            log.error(errorMsg, e);
            throw new KafkaException(errorMsg, e);
        }
    }

    /**
     * Creates a complete Properties object with all Kafka consumer configuration.
     * Includes both the core configuration (brokers, topic, consumer group) and 
     * any additional properties from KAFKA_CONSUMER_* environment variables.
     * @return Complete Properties object for Kafka consumer configuration
     */
    public static Properties getAllKafkaProperties() {
        Properties properties = getKafkaConsumerProperties();
        
        // Add core configuration
        properties.setProperty("bootstrap.servers", getKafkaBrokers());
        properties.setProperty("group.id", getConsumerGroup());
        
        // Set default properties if not already specified
        if (!properties.containsKey("key.deserializer")) {
            properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }
        if (!properties.containsKey("value.deserializer")) {
            properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }
        
        return properties;
    }
}