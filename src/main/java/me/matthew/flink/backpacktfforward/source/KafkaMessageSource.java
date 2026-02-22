package me.matthew.flink.backpacktfforward.source;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.config.KafkaConfiguration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.kafka.common.KafkaException;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

/**
 * Factory class for creating configured Kafka source instances.
 * Handles Kafka source configuration using environment variables and provides
 * a properly configured KafkaSource for consuming messages from the specified topic.
 */
@Slf4j
public class KafkaMessageSource {

    /**
     * Creates a configured KafkaSource instance for consuming string messages.
     * The source is configured with:
     * - String deserialization for Kafka message values
     * - Consumer group and topic subscription from environment variables
     * - Additional consumer properties from KAFKA_CONSUMER_* environment variables
     * - Automatic offset initialization (latest for new consumer groups, or from KAFKA_START_TIMESTAMP if set)
     * - Retry policies for broker connectivity issues
     * - Graceful shutdown with proper offset commits
     * 
     * @return Configured KafkaSource<String> ready for use in Flink streaming job
     * @throws IllegalArgumentException if required Kafka configuration is missing or invalid
     * @throws KafkaException if broker connectivity or topic validation fails
     */
    public static KafkaSource<String> createSource() {
        log.info("Creating Kafka source...");
        
        try {
            // Check if start timestamp is configured
            Long startTimestamp = KafkaConfiguration.getStartTimestamp();
            
            if (startTimestamp != null) {
                log.info("Using configured start timestamp: {} ({})", 
                        startTimestamp, Instant.ofEpochMilli(startTimestamp));
                return createSourceFromTimestamp(startTimestamp);
            } else {
                log.info("No start timestamp configured, using latest offset");
                return createSourceWithLatestOffset();
            }
            
        } catch (KafkaException e) {
            log.error("Failed to create Kafka source due to connectivity issues: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error creating Kafka source", e);
            throw new KafkaException("Failed to create Kafka source", e);
        }
    }
    
    /**
     * Creates a configured KafkaSource instance starting from latest offset.
     * This is the original behavior extracted into a separate method.
     * 
     * @return Configured KafkaSource<String> starting from latest offset
     * @throws IllegalArgumentException if required Kafka configuration is missing or invalid
     * @throws KafkaException if broker connectivity or topic validation fails
     */
    public static KafkaSource<String> createSourceWithLatestOffset() {
        log.info("Creating Kafka source with latest offset...");
        
        return createSource(OffsetsInitializer.latest());
    }
    
    /**
     * Creates a configured KafkaSource instance starting from a specific timestamp.
     * The source will start consuming from the first message at or after the specified timestamp.
     * 
     * @param startTimestamp Unix timestamp in milliseconds to start consuming from
     * @return Configured KafkaSource<String> starting from the specified timestamp
     * @throws IllegalArgumentException if required Kafka configuration is missing or invalid
     * @throws KafkaException if broker connectivity or topic validation fails
     */
    public static KafkaSource<String> createSourceFromTimestamp(long startTimestamp) {
        log.info("Creating Kafka source starting from timestamp: {} ({})", 
                startTimestamp, Instant.ofEpochMilli(startTimestamp));
        
        return createSource(OffsetsInitializer.timestamp(startTimestamp));
    }
    
    /**
     * Creates a configured KafkaSource instance starting from a specific timestamp.
     * The source will start consuming from the first message at or after the specified timestamp.
     * 
     * @param startTimestamp Instant representing the timestamp to start consuming from
     * @return Configured KafkaSource<String> starting from the specified timestamp
     * @throws IllegalArgumentException if required Kafka configuration is missing or invalid
     * @throws KafkaException if broker connectivity or topic validation fails
     */
    public static KafkaSource<String> createSourceFromTimestamp(Instant startTimestamp) {
        log.info("Creating Kafka source starting from timestamp: {}", startTimestamp);
        
        return createSource(OffsetsInitializer.timestamp(startTimestamp.toEpochMilli()));
    }

    /**
     * Creates a configured KafkaSource instance with custom offset initialization.
     * Allows specifying how to initialize offsets for new consumer groups.
     * 
     * @param offsetsInitializer Strategy for initializing offsets (earliest, latest, specific offsets, etc.)
     * @return Configured KafkaSource<String> with custom offset initialization
     * @throws IllegalArgumentException if required Kafka configuration is missing or invalid
     * @throws KafkaException if broker connectivity or topic validation fails
     */
    public static KafkaSource<String> createSource(OffsetsInitializer offsetsInitializer) {
        log.info("Creating Kafka source with custom offset initializer...");
        
        try {
            // Validate configuration first
            KafkaConfiguration.validateConfiguration();
            
            // Get configuration values
            String kafkaBrokers = KafkaConfiguration.getKafkaBrokers();
            String kafkaTopic = KafkaConfiguration.getKafkaTopic();
            String consumerGroup = KafkaConfiguration.getConsumerGroup();
            Properties kafkaProperties = KafkaConfiguration.getKafkaConsumerProperties();
            
            // Add resilience and error handling properties
            enhancePropertiesForResilience(kafkaProperties);
            
            // Add consumer group coordination and monitoring properties
            enhancePropertiesForCoordination(kafkaProperties);
            
            log.info("Configuring Kafka source with brokers: {}, topic: {}, consumer group: {}", 
                    kafkaBrokers, kafkaTopic, consumerGroup);
            
            // Build the KafkaSource with custom offset initializer and enhanced error handling
            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                    .setBootstrapServers(kafkaBrokers)
                    .setTopics(kafkaTopic)
                    .setGroupId(consumerGroup)
                    .setStartingOffsets(offsetsInitializer)
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .setProperties(kafkaProperties)
                    .build();
            
            log.info("Kafka source created successfully with custom offset initializer and enhanced error handling");
            return kafkaSource;
            
        } catch (KafkaException e) {
            log.error("Failed to create Kafka source due to connectivity issues: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error creating Kafka source with custom offset initializer", e);
            throw new KafkaException("Failed to create Kafka source", e);
        }
    }

    /**
     * Enhances Kafka consumer properties with resilience and error handling configurations.
     * Sets retry policies, timeouts, and other properties for robust operation.
     * 
     * @param properties Properties object to enhance (modified in place)
     */
    private static void enhancePropertiesForResilience(Properties properties) {
        log.debug("Enhancing Kafka properties for resilience...");
        
        // Connection and retry settings (only set if not already configured)
        setPropertyIfNotExists(properties, "reconnect.backoff.ms", "1000"); // 1 second initial backoff
        setPropertyIfNotExists(properties, "reconnect.backoff.max.ms", "32000"); // 32 second max backoff
        setPropertyIfNotExists(properties, "retry.backoff.ms", "1000"); // 1 second retry backoff
        setPropertyIfNotExists(properties, "request.timeout.ms", "60000"); // 60 second request timeout (match Bridge)
        setPropertyIfNotExists(properties, "connections.max.idle.ms", "300000"); // 5 minute idle timeout
        
        // Session and heartbeat settings for consumer group coordination
        setPropertyIfNotExists(properties, "session.timeout.ms", "45000"); // 45 second session timeout (allow more time before rebalancing)
        setPropertyIfNotExists(properties, "heartbeat.interval.ms", "3000"); // 3 second heartbeat interval (keep session alive)
        setPropertyIfNotExists(properties, "max.poll.interval.ms", "600000"); // 10 minute max poll interval
        
        // Offset commit settings for graceful shutdown
        setPropertyIfNotExists(properties, "enable.auto.commit", "true"); // Enable auto commit
        setPropertyIfNotExists(properties, "auto.commit.interval.ms", "5000"); // 5 second commit interval
        
        // Fetch settings for performance and reliability
        setPropertyIfNotExists(properties, "fetch.min.bytes", "1"); // Minimum fetch size
        setPropertyIfNotExists(properties, "fetch.max.wait.ms", "500"); // Don't hammer the broker for tiny data
        
        // Error handling settings
        setPropertyIfNotExists(properties, "retries", "3"); // Number of retries for transient errors
        
        log.debug("Kafka properties enhanced for resilience");
    }

    /**
     * Enhances Kafka consumer properties with consumer group coordination and monitoring configurations.
     * Sets properties for rebalancing behavior, offset management, and monitoring.
     * 
     * @param properties Properties object to enhance (modified in place)
     */
    private static void enhancePropertiesForCoordination(Properties properties) {
        log.debug("Enhancing Kafka properties for consumer group coordination...");
        
        // Consumer group coordination settings
        setPropertyIfNotExists(properties, "partition.assignment.strategy", 
    "org.apache.kafka.clients.consumer.CooperativeStickyAssignor,org.apache.kafka.clients.consumer.RangeAssignor");
        
        // Rebalancing behavior settings
        setPropertyIfNotExists(properties, "max.poll.records", "50"); // Limit records per poll to avoid rebalancing
        setPropertyIfNotExists(properties, "session.timeout.ms", "60000"); // 60 second session timeout
        setPropertyIfNotExists(properties, "heartbeat.interval.ms", "3000"); // 3 second heartbeat interval
        
        // Offset management settings for better coordination
        setPropertyIfNotExists(properties, "auto.offset.reset", "latest"); // Start from latest if no committed offset
        setPropertyIfNotExists(properties, "enable.auto.commit", "true"); // Enable automatic offset commits
        setPropertyIfNotExists(properties, "auto.commit.interval.ms", "5000"); // Commit every 5 seconds
        
        // Consumer group metadata settings
        // Note: group.instance.id is not set by default to use dynamic membership
        // If static membership is needed, set KAFKA_CONSUMER_GROUP_INSTANCE_ID environment variable
        
        // Monitoring and logging settings
        setPropertyIfNotExists(properties, "client.id", "backpack-tf-flink-consumer"); // Client identifier for monitoring
        
        log.debug("Kafka properties enhanced for consumer group coordination");
    }

    /**
     * Sets a property value only if it doesn't already exist in the properties.
     * This allows user-configured values to take precedence over defaults.
     * 
     * @param properties Properties object to modify
     * @param key Property key
     * @param defaultValue Default value to set if key doesn't exist
     */
    private static void setPropertyIfNotExists(Properties properties, String key, String defaultValue) {
        if (!properties.containsKey(key)) {
            properties.setProperty(key, defaultValue);
            log.trace("Set default Kafka property: {} = {}", key, defaultValue);
        } else {
            log.trace("Using existing Kafka property: {} = {}", key, properties.getProperty(key));
        }
    }
}