package me.matthew.flink.backpacktfforward;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.config.ApiConfiguration;
import me.matthew.flink.backpacktfforward.config.BackpackTfApiConfiguration;
import me.matthew.flink.backpacktfforward.config.SteamApiConfiguration;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;
import me.matthew.flink.backpacktfforward.parser.BackfillMessageParser;
import me.matthew.flink.backpacktfforward.processor.BackfillProcessor;
import me.matthew.flink.backpacktfforward.sink.ListingDeleteSink;
import me.matthew.flink.backpacktfforward.sink.ListingUpsertSink;
import me.matthew.flink.backpacktfforward.parser.KafkaMessageParser;
import me.matthew.flink.backpacktfforward.source.BackfillRequestSource;
import me.matthew.flink.backpacktfforward.source.BackfillSourceWithMetrics;
import me.matthew.flink.backpacktfforward.source.KafkaMessageSource;
import me.matthew.flink.backpacktfforward.source.KafkaSourceWithMetrics;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.KafkaException;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.INCOMING_EVENTS;

@Slf4j
public class WebSocketForwarderJob {

    public static void main(String[] args) throws Exception {
        log.info("Starting BackpackTF Kafka Forwarder Job...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read env vars safely
        String dbUrl = System.getenv("DB_URL");
        String dbUser = System.getenv("DB_USERNAME");
        String dbPass = System.getenv("DB_PASSWORD");
        int upsertBatchSize = Integer.parseInt(System.getenv().getOrDefault("UPSERT_BATCH_SIZE", "10"));
        long upsertBatchIntervalMs = Long.parseLong(System.getenv().getOrDefault("UPSERT_BATCH_INTERVAL_MS", "200"));
        int deleteBatchSize = Integer.parseInt(System.getenv().getOrDefault("DELETE_BATCH_SIZE", "10"));
        long deleteBatchIntervalMs = Long.parseLong(System.getenv().getOrDefault("DELETE_BATCH_INTERVAL_MS", "1000"));

        log.debug("Upsert batch size: {}", upsertBatchSize);
        log.debug("Upsert batch interval (ms): {}", upsertBatchIntervalMs);
        log.debug("Delete batch size: {}", deleteBatchSize);
        log.debug("Delete batch interval (ms): {}", deleteBatchIntervalMs);

        if (dbUrl == null || dbUser == null || dbPass == null)
            throw new IllegalArgumentException("Database env vars missing");

        // Validate backfill configuration (optional functionality)
        boolean backfillEnabled = validateBackfillConfiguration();

        // Create Kafka source with error handling
        KafkaSource<String> kafkaSource;
        try {
            kafkaSource = KafkaMessageSource.createSource();
        } catch (KafkaException e) {
            log.error("Failed to create Kafka source. Please check Kafka configuration and connectivity.", e);
            throw new IllegalStateException("Kafka source creation failed: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            log.error("Invalid Kafka configuration. Please check environment variables.", e);
            throw e;
        }

        DataStreamSource<String> source = env.fromSource(kafkaSource, 
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), 
                "BackpackTFKafkaSource");

        // Add Kafka-specific metrics collection
        var sourceWithMetrics = KafkaSourceWithMetrics.addMetrics(source)
                .name("BackpackTFKafkaSourceWithMetrics");

        var parsed = sourceWithMetrics
                .flatMap(new KafkaMessageParser())
                .returns(ListingUpdate.class)
                .name("BackpackTFKafkaMessageParser");

        // Create backfill source with error handling (optional - only if backfill is configured)
        DataStream<ListingUpdate> backfillStream = null;
        if (backfillEnabled) {
            try {
                log.info("Backfill configuration validated, enabling backfill functionality");
                
                // Create backfill Kafka source with error handling
                KafkaSource<String> backfillKafkaSource;
                try {
                    backfillKafkaSource = BackfillRequestSource.createKafkaSource();
                } catch (Exception e) {
                    log.error("Failed to create backfill Kafka source: {}. Backfill functionality will be disabled.", e.getMessage(), e);
                    throw new IllegalStateException("Backfill Kafka source creation failed", e);
                }
                
                DataStreamSource<String> backfillSource = env.fromSource(backfillKafkaSource,
                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                        "BackpackTFBackfillKafkaSource");
                
                // Parse backfill messages to BackfillRequest objects with error handling
                var backfillRequests = backfillSource
                        .flatMap(new BackfillMessageParser())
                        .returns(BackfillRequest.class)
                        .name("BackpackTFBackfillMessageParser");
                
                // Add backfill-specific metrics collection
                var backfillRequestsWithMetrics = BackfillSourceWithMetrics.addMetrics(backfillRequests)
                        .name("BackpackTFBackfillSourceWithMetrics");
                
                // Process backfill requests through BackfillProcessor with error handling
                try {
                    backfillStream = backfillRequestsWithMetrics
                            .flatMap(new BackfillProcessor(dbUrl, dbUser, dbPass))
                            .returns(ListingUpdate.class)
                            .name("BackpackTFBackfillProcessor");
                } catch (Exception e) {
                    log.error("Failed to create BackfillProcessor: {}. Backfill functionality will be disabled.", e.getMessage(), e);
                    throw new IllegalStateException("BackfillProcessor creation failed", e);
                }
                
                log.info("Backfill source and processor configured successfully");
            } catch (Exception e) {
                log.error("Failed to initialize backfill functionality: {}. " +
                         "The job will continue with Kafka-only functionality.", e.getMessage(), e);
                
                // Decide whether to fail the job or continue without backfill
                // For resilience, we'll continue without backfill rather than failing the entire job
                log.warn("Continuing job execution without backfill functionality due to initialization failure");
                backfillStream = null;
            }
        }

        // Combine Kafka and backfill streams (if backfill is available)
        DataStream<ListingUpdate> allEvents;
        if (backfillStream != null) {
            allEvents = parsed.union(backfillStream);
            log.info("Combined Kafka and backfill event streams");
        } else {
            allEvents = parsed;
            log.info("Using Kafka event stream only");
        }

        allEvents.map(new RichMapFunction<ListingUpdate, ListingUpdate>() {

            private Counter incomingEvents;

            @Override
            public ListingUpdate map(ListingUpdate listingUpdate) throws Exception {
                incomingEvents.inc();
                return listingUpdate;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                incomingEvents =
                        getRuntimeContext()
                                .getMetricGroup()
                                .counter(INCOMING_EVENTS);
            }
        });

        // Route events to appropriate sinks
        allEvents.filter(lu -> lu != null && lu.getEvent() != null && lu.getEvent().equals("listing-update"))
                .name("BackpackTFListingUpdateFilter")
                .addSink(new ListingUpsertSink(dbUrl, dbUser, dbPass, upsertBatchSize, upsertBatchIntervalMs))
                .name("BackpackTFListingUpsertSink");

        allEvents.filter(lu -> lu != null && lu.getEvent() != null && lu.getEvent().equals("listing-delete"))
                .name("BackpackTFListingDeleteFilter")
                .addSink(new ListingDeleteSink(dbUrl, dbUser, dbPass, deleteBatchSize, deleteBatchIntervalMs))
                .name("BackpackTFListingDeleteSink");

        log.info("Starting Flink job execution...");
        env.execute("BackpackTF Kafka Forwarder");
    }

    /**
     * Validates backfill configuration and determines if backfill functionality should be enabled.
     * Backfill is optional functionality that requires specific environment variables to be set.
     * Uses centralized configuration validation for API clients.
     * 
     * @return true if backfill should be enabled, false otherwise
     */
    private static boolean validateBackfillConfiguration() {
        log.info("Validating backfill configuration...");
        
        try {
            // Check required backfill Kafka environment variables
            String backfillTopic = System.getenv("BACKFILL_KAFKA_TOPIC");
            String backfillConsumerGroup = System.getenv("BACKFILL_KAFKA_CONSUMER_GROUP");
            
            // If any required Kafka variable is missing, backfill is disabled
            if (backfillTopic == null || backfillTopic.trim().isEmpty()) {
                log.info("BACKFILL_KAFKA_TOPIC not set, backfill functionality disabled");
                return false;
            }
            
            if (backfillConsumerGroup == null || backfillConsumerGroup.trim().isEmpty()) {
                log.info("BACKFILL_KAFKA_CONSUMER_GROUP not set, backfill functionality disabled");
                return false;
            }
            
            // Validate API configurations using centralized configuration classes
            try {
                SteamApiConfiguration.validateConfiguration();
                
                // Validate that backfill Kafka configuration is complete
                BackfillRequestSource.getBackfillKafkaTopic();
                BackfillRequestSource.getBackfillKafkaConsumerGroup();
                
                log.info("Backfill configuration validation successful:");
                log.info("  Backfill Topic: {}", backfillTopic);
                log.info("  Backfill Consumer Group: {}", backfillConsumerGroup);
                log.info("  BackpackTF API: CONFIGURED");
                log.info("  Steam API: CONFIGURED");
                return true;
                
            } catch (IllegalArgumentException e) {
                log.warn("Backfill API configuration validation failed: {}", e.getMessage());
                log.info("Backfill functionality disabled due to missing API configuration");
                return false;
            }
            
        } catch (Exception e) {
            log.warn("Unexpected error during backfill configuration validation: {}", e.getMessage());
            return false;
        }
    }
}
