package me.matthew.flink.backpacktfforward.source;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.config.KafkaConfiguration;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.common.KafkaException;

import java.util.Properties;

/**
 * Factory class for creating Kafka sources for backfill request messages.
 * Uses existing KafkaConfiguration patterns and provides configured KafkaSource instances.
 * Follows the same patterns as existing Kafka sources in the codebase.
 */
@Slf4j
public class BackfillRequestSource {
    
    private static final String BACKFILL_KAFKA_TOPIC_ENV = "BACKFILL_KAFKA_TOPIC";
    private static final String BACKFILL_KAFKA_CONSUMER_GROUP_ENV = "BACKFILL_KAFKA_CONSUMER_GROUP";
    
    // Private constructor to prevent instantiation
    private BackfillRequestSource() {
    }
    
    /**
     * Creates a configured KafkaSource for consuming backfill request messages.
     * Uses existing KafkaConfiguration patterns with backfill-specific topic and consumer group.
     * 
     * @return Configured KafkaSource for backfill requests
     * @throws IllegalArgumentException if required configuration is missing
     * @throws KafkaException if Kafka connectivity fails
     */
    public static KafkaSource<String> createKafkaSource() {
        log.info("Creating backfill Kafka source...");
        
        try {
            // Get backfill-specific configuration
            String backfillTopic = getBackfillKafkaTopic();
            String backfillConsumerGroup = getBackfillKafkaConsumerGroup();
            
            // Use existing Kafka configuration for brokers and additional properties
            String kafkaBrokers = KafkaConfiguration.getKafkaBrokers();
            Properties kafkaProperties = KafkaConfiguration.getKafkaConsumerProperties();
            
            // Enhance properties for resilience (reuse existing patterns)
            enhancePropertiesForResilience(kafkaProperties);
            
            log.info("Configuring backfill Kafka source with brokers: {}, topic: {}, consumer group: {}", 
                    kafkaBrokers, backfillTopic, backfillConsumerGroup);
            
            // Build the KafkaSource for backfill requests
            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                    .setBootstrapServers(kafkaBrokers)
                    .setTopics(backfillTopic)
                    .setGroupId(backfillConsumerGroup)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .setProperties(kafkaProperties)
                    .build();
            
            log.info("Backfill Kafka source created successfully");
            return kafkaSource;
            
        } catch (Exception e) {
            log.error("Failed to create backfill Kafka source", e);
            throw new KafkaException("Failed to create backfill Kafka source", e);
        }
    }
    
    /**
     * Reads backfill Kafka topic from environment variable.
     * @return Backfill Kafka topic name
     * @throws IllegalArgumentException if BACKFILL_KAFKA_TOPIC is not set or empty
     */
    public static String getBackfillKafkaTopic() {
        String topic = System.getenv(BACKFILL_KAFKA_TOPIC_ENV);
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s is required but not set or empty", BACKFILL_KAFKA_TOPIC_ENV)
            );
        }
        return topic.trim();
    }
    
    /**
     * Reads backfill Kafka consumer group from environment variable.
     * @return Backfill Kafka consumer group ID
     * @throws IllegalArgumentException if BACKFILL_KAFKA_CONSUMER_GROUP is not set or empty
     */
    public static String getBackfillKafkaConsumerGroup() {
        String consumerGroup = System.getenv(BACKFILL_KAFKA_CONSUMER_GROUP_ENV);
        if (consumerGroup == null || consumerGroup.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Environment variable %s is required but not set or empty", BACKFILL_KAFKA_CONSUMER_GROUP_ENV)
            );
        }
        return consumerGroup.trim();
    }
    
    /**
     * Validates that all required backfill configuration is present.
     * @throws IllegalArgumentException if any required configuration is missing
     */
    public static void validateConfiguration() {
        log.info("Validating backfill configuration...");
        
        try {
            // Validate backfill-specific configuration
            String backfillTopic = getBackfillKafkaTopic();
            String backfillConsumerGroup = getBackfillKafkaConsumerGroup();
            
            // Validate existing Kafka configuration
            KafkaConfiguration.validateConfiguration();
            
            log.info("Backfill configuration validated successfully:");
            log.info("  Backfill Topic: {}", backfillTopic);
            log.info("  Backfill Consumer Group: {}", backfillConsumerGroup);
            
        } catch (IllegalArgumentException e) {
            log.error("Backfill configuration validation failed: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Backfill configuration validation failed: {}", e.getMessage());
            throw new KafkaException("Failed to validate backfill configuration", e);
        }
    }
    
    /**
     * Enhances Kafka consumer properties with resilience and error handling configurations.
     * Reuses the same patterns from existing KafkaMessageSource.
     * 
     * @param properties Properties object to enhance (modified in place)
     */
    private static void enhancePropertiesForResilience(Properties properties) {
        log.debug("Enhancing backfill Kafka properties for resilience...");
        
        // Connection and retry settings (only set if not already configured)
        setPropertyIfNotExists(properties, "reconnect.backoff.ms", "1000");
        setPropertyIfNotExists(properties, "reconnect.backoff.max.ms", "32000");
        setPropertyIfNotExists(properties, "retry.backoff.ms", "1000");
        setPropertyIfNotExists(properties, "request.timeout.ms", "60000"); // Match Bridge timeout
        setPropertyIfNotExists(properties, "connections.max.idle.ms", "300000");
        
        // Session and heartbeat settings
        setPropertyIfNotExists(properties, "session.timeout.ms", "45000"); // Allow more time before rebalancing
        setPropertyIfNotExists(properties, "heartbeat.interval.ms", "3000"); // Keep the session alive
        setPropertyIfNotExists(properties, "max.poll.interval.ms", "300000");
        
        // Offset commit settings
        setPropertyIfNotExists(properties, "enable.auto.commit", "true");
        setPropertyIfNotExists(properties, "auto.commit.interval.ms", "5000");
        
        // Fetch settings
        setPropertyIfNotExists(properties, "fetch.min.bytes", "1");
        setPropertyIfNotExists(properties, "fetch.max.wait.ms", "500"); // Don't hammer the broker for tiny data
        
        // Error handling settings
        setPropertyIfNotExists(properties, "retries", "3");
        setPropertyIfNotExists(properties, "auto.offset.reset", "latest");
        
        // Consumer group settings
        setPropertyIfNotExists(properties, "partition.assignment.strategy", 
            "org.apache.kafka.clients.consumer.RangeAssignor,org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        setPropertyIfNotExists(properties, "max.poll.records", "500");
        
        // Client identification
        setPropertyIfNotExists(properties, "client.id", "backpack-tf-backfill-consumer");
        
        log.debug("Backfill Kafka properties enhanced for resilience");
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
            log.trace("Set default backfill Kafka property: {} = {}", key, defaultValue);
        } else {
            log.trace("Using existing backfill Kafka property: {} = {}", key, properties.getProperty(key));
        }
    }
}