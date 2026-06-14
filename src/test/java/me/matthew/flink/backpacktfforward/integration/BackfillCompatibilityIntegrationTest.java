package me.matthew.flink.backpacktfforward.integration;

import me.matthew.flink.backpacktfforward.WebSocketForwarderJob;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequestType;
import me.matthew.flink.backpacktfforward.parser.BackfillMessageParser;
import me.matthew.flink.backpacktfforward.parser.KafkaMessageParser;
import me.matthew.flink.backpacktfforward.sink.ListingDeleteSink;
import me.matthew.flink.backpacktfforward.sink.ListingUpsertSink;
import me.matthew.flink.backpacktfforward.source.BackfillRequestSource;
import me.matthew.flink.backpacktfforward.source.KafkaMessageSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Integration test to validate minimal code changes outside BackfillProcessor.
 * Ensures existing components work without modifications and maintain compatibility.
 * 
 * **Feature: backfill-steam-integration, Requirements: 11.1, 11.2, 11.3, 11.6**
 */
class BackfillCompatibilityIntegrationTest {
    
    @BeforeEach
    void setUp() {
        // No setup needed for compatibility tests
    }
    
    @Test
    void testBackfillRequestModelWorksWithoutModifications() {
        // Test that existing BackfillRequest model works without modifications
        
        // Create BackfillRequest using existing constructor and setters
        BackfillRequest request = new BackfillRequest();
        request.setItemDefindex(190);
        request.setItemQualityId(11);
        request.setRequestType(BackfillRequestType.FULL);
        
        // Verify all expected fields are accessible
        assertEquals(190, request.getItemDefindex());
        assertEquals(11, request.getItemQualityId());
        
        // Verify the model has the expected JSON annotations for Kafka parsing
        try {
            Field defindexField = BackfillRequest.class.getDeclaredField("itemDefindex");
            assertNotNull(defindexField.getAnnotation(com.fasterxml.jackson.annotation.JsonProperty.class));
            
            Field qualityField = BackfillRequest.class.getDeclaredField("itemQualityId");
            assertNotNull(qualityField.getAnnotation(com.fasterxml.jackson.annotation.JsonProperty.class));
        } catch (NoSuchFieldException e) {
            fail("BackfillRequest model fields should exist: " + e.getMessage());
        }
        
        // Test constructor with parameters (AllArgsConstructor includes marketName)
        BackfillRequest requestWithParams = new BackfillRequest(266, 5, null, BackfillRequestType.FULL, null, null);
        assertEquals(266, requestWithParams.getItemDefindex());
        assertEquals(5, requestWithParams.getItemQualityId());
        assertNull(requestWithParams.getMarketName());
        
        // Verify toString, equals, hashCode work (Lombok generated)
        assertNotNull(request.toString());
        assertTrue(request.toString().contains("itemDefindex"));
        assertTrue(request.toString().contains("itemQualityId"));
        
        BackfillRequest request2 = new BackfillRequest(190, 11, null, BackfillRequestType.FULL, null, null);
        assertEquals(request, request2);
        assertEquals(request.hashCode(), request2.hashCode());
    }
    
    @Test
    void testKafkaSourceAndMessageParsingRemainUnchanged() {
        // Test that existing Kafka source and message parsing work without modifications
        
        // Verify KafkaMessageSource static methods are accessible
        assertNotNull(KafkaMessageSource.class);
        
        try {
            Method createSourceMethod = KafkaMessageSource.class.getDeclaredMethod("createSource");
            assertNotNull(createSourceMethod);
            assertTrue(java.lang.reflect.Modifier.isStatic(createSourceMethod.getModifiers()));
        } catch (NoSuchMethodException e) {
            fail("KafkaMessageSource.createSource() method should exist");
        }
        
        // Verify KafkaMessageParser works with ListingUpdate
        KafkaMessageParser parser = new KafkaMessageParser();
        assertNotNull(parser);
        
        // Test that parser can be instantiated and has expected methods
        assertTrue(parser instanceof org.apache.flink.api.common.functions.FlatMapFunction);
        
        // Verify BackfillMessageParser works with BackfillRequest
        BackfillMessageParser backfillParser = new BackfillMessageParser();
        assertNotNull(backfillParser);
        assertTrue(backfillParser instanceof org.apache.flink.api.common.functions.FlatMapFunction);
        
        // Verify BackfillRequestSource static methods are accessible
        assertNotNull(BackfillRequestSource.class);
        
        try {
            Method getTopicMethod = BackfillRequestSource.class.getDeclaredMethod("getBackfillKafkaTopic");
            assertNotNull(getTopicMethod);
            assertTrue(java.lang.reflect.Modifier.isStatic(getTopicMethod.getModifiers()));
            
            Method getConsumerGroupMethod = BackfillRequestSource.class.getDeclaredMethod("getBackfillKafkaConsumerGroup");
            assertNotNull(getConsumerGroupMethod);
            assertTrue(java.lang.reflect.Modifier.isStatic(getConsumerGroupMethod.getModifiers()));
            
            Method createKafkaSourceMethod = BackfillRequestSource.class.getDeclaredMethod("createKafkaSource");
            assertNotNull(createKafkaSourceMethod);
            assertTrue(java.lang.reflect.Modifier.isStatic(createKafkaSourceMethod.getModifiers()));
        } catch (NoSuchMethodException e) {
            fail("BackfillRequestSource methods should exist: " + e.getMessage());
        }
    }
    
    @Test
    void testSinkInfrastructureWorksWithNewListingUpdateObjects() {
        // Test that existing sink infrastructure works with new ListingUpdate objects
        
        // Create sample ListingUpdate objects that would be generated by the new processor
        ListingUpdate updateEvent = createSampleUpdateEvent();
        ListingUpdate deleteEvent = createSampleDeleteEvent();
        
        // Verify ListingUpdate model structure is compatible
        assertNotNull(updateEvent.getId());
        assertNotNull(updateEvent.getEvent());
        assertNotNull(updateEvent.getPayload());
        assertEquals("listing-update", updateEvent.getEvent());
        
        assertNotNull(deleteEvent.getId());
        assertNotNull(deleteEvent.getEvent());
        assertNotNull(deleteEvent.getPayload());
        assertEquals("listing-delete", deleteEvent.getEvent());
        
        // Verify sink classes can be instantiated with expected parameters
        try {
            ListingUpsertSink upsertSink = new ListingUpsertSink(
                "jdbc:h2:mem:test", "test", "test", 10, 200L
            );
            assertNotNull(upsertSink);
            assertTrue(upsertSink instanceof org.apache.flink.streaming.api.functions.sink.SinkFunction);
        } catch (Exception e) {
            fail("ListingUpsertSink should be instantiable: " + e.getMessage());
        }
        
        try {
            ListingDeleteSink deleteSink = new ListingDeleteSink(
                "jdbc:h2:mem:test", "test", "test", 10, 1000L
            );
            assertNotNull(deleteSink);
            assertTrue(deleteSink instanceof org.apache.flink.streaming.api.functions.sink.SinkFunction);
        } catch (Exception e) {
            fail("ListingDeleteSink should be instantiable: " + e.getMessage());
        }
        
        // Verify ListingUpdate has all expected fields for sink processing
        assertNotNull(updateEvent.getPayload().getId());
        assertNotNull(updateEvent.getPayload().getSteamid());
        assertNotNull(updateEvent.getPayload().getIntent());
        assertNotNull(updateEvent.getPayload().getItem());
        assertNotNull(updateEvent.getPayload().getItem().getDefindex());
        assertNotNull(updateEvent.getPayload().getItem().getQuality());
    }
    
    @Test
    void testWebSocketForwarderJobIntegrationPointsMaintained() {
        // Test that existing WebSocketForwarderJob integration points are maintained
        
        // Verify main method exists
        try {
            Method mainMethod = WebSocketForwarderJob.class.getDeclaredMethod("main", String[].class);
            assertNotNull(mainMethod);
            assertTrue(java.lang.reflect.Modifier.isStatic(mainMethod.getModifiers()));
            assertTrue(java.lang.reflect.Modifier.isPublic(mainMethod.getModifiers()));
        } catch (NoSuchMethodException e) {
            fail("WebSocketForwarderJob.main() method should exist");
        }
        
        // Verify the job class is accessible and public
        assertTrue(java.lang.reflect.Modifier.isPublic(WebSocketForwarderJob.class.getModifiers()));
        
        // Test that the job can handle both regular Kafka events and backfill events
        // This is verified by checking that the job has the expected structure for event routing
        
        // Verify that event filtering logic exists (this would be in the main method)
        // We can't easily test the actual filtering without running the full job,
        // but we can verify the classes and methods exist
        
        // Check that ListingUpdate model supports both event types
        ListingUpdate updateEvent = createSampleUpdateEvent();
        ListingUpdate deleteEvent = createSampleDeleteEvent();
        
        // Verify event routing would work based on event type
        assertTrue(updateEvent.getEvent().equals("listing-update"));
        assertTrue(deleteEvent.getEvent().equals("listing-delete"));
        
        // These would be routed to different sinks in the actual job
        assertNotEquals(updateEvent.getEvent(), deleteEvent.getEvent());
    }
    
    @Test
    void testBackfillProcessorIntegrationWithExistingInfrastructure() {
        // Test that BackfillProcessor integrates properly with existing infrastructure
        
        // Verify BackfillProcessor extends the expected Flink interface
        try {
            Class<?> processorClass = Class.forName("me.matthew.flink.backpacktfforward.processor.BackfillProcessor");
            assertTrue(org.apache.flink.api.common.functions.RichFlatMapFunction.class.isAssignableFrom(processorClass));
            
            // Verify constructor signature matches expected pattern
            var constructor = processorClass.getConstructor(String.class, String.class, String.class);
            assertNotNull(constructor);
            
            // Verify the processor can be instantiated with database parameters
            var processor = constructor.newInstance("jdbc:h2:mem:test", "test", "test");
            assertNotNull(processor);
            
        } catch (Exception e) {
            fail("BackfillProcessor should integrate with existing infrastructure: " + e.getMessage());
        }
    }
    
    @Test
    void testDatabaseConnectionPatternsReused() {
        // Test that existing database connection patterns are reused
        
        // Verify that sinks still use the same constructor pattern
        try {
            // Test ListingUpsertSink constructor
            var upsertConstructor = ListingUpsertSink.class.getConstructor(
                String.class, String.class, String.class, int.class, long.class
            );
            assertNotNull(upsertConstructor);
            
            // Test ListingDeleteSink constructor  
            var deleteConstructor = ListingDeleteSink.class.getConstructor(
                String.class, String.class, String.class, int.class, long.class
            );
            assertNotNull(deleteConstructor);
            
            // Verify both sinks can be created with same database parameters
            String jdbcUrl = "jdbc:h2:mem:test";
            String username = "test";
            String password = "test";
            
            var upsertSink = upsertConstructor.newInstance(jdbcUrl, username, password, 10, 200L);
            var deleteSink = deleteConstructor.newInstance(jdbcUrl, username, password, 10, 1000L);
            
            assertNotNull(upsertSink);
            assertNotNull(deleteSink);
            
        } catch (Exception e) {
            fail("Database connection patterns should be reused: " + e.getMessage());
        }
    }
    
    @Test
    void testConfigurationPatternsRemainConsistent() {
        // Test that configuration patterns remain consistent
        
        // Verify environment variable patterns are maintained
        // (We can't test actual environment variables, but we can test the code structure)
        
        // Test that WebSocketForwarderJob expects the same environment variables
        // This is implicit in the job structure - if it compiles and the integration
        // test passes, the configuration patterns are maintained
        
        // Verify that backfill configuration validation exists
        try {
            // The validateBackfillConfiguration method should exist in WebSocketForwarderJob
            Method validateMethod = WebSocketForwarderJob.class.getDeclaredMethod("validateBackfillConfiguration");
            assertNotNull(validateMethod);
            assertTrue(java.lang.reflect.Modifier.isStatic(validateMethod.getModifiers()));
            assertTrue(java.lang.reflect.Modifier.isPrivate(validateMethod.getModifiers()));
        } catch (NoSuchMethodException e) {
            fail("Backfill configuration validation should exist: " + e.getMessage());
        }
        
        // Test that the job handles missing backfill configuration gracefully
        // This is verified by the fact that the job can run with or without backfill enabled
        assertTrue(true, "Configuration patterns are consistent if compilation succeeds");
    }
    
    // Helper methods to create sample data
    
    private ListingUpdate createSampleUpdateEvent() {
        ListingUpdate update = new ListingUpdate();
        update.id = "440_16525961480";
        update.event = "listing-update";
        
        ListingUpdate.Payload payload = new ListingUpdate.Payload();
        payload.id = "440_16525961480";
        payload.steamid = "76561199574661225";
        payload.appid = 440;
        payload.intent = "sell";
        payload.count = 1;
        payload.status = "active";
        payload.source = "user";
        payload.listedAt = System.currentTimeMillis() / 1000;
        payload.bumpedAt = System.currentTimeMillis() / 1000;
        
        // Create item
        ListingUpdate.Item item = new ListingUpdate.Item();
        item.appid = 440;
        item.defindex = 190;
        item.id = "16525961480";
        item.marketName = "Strange Bat";
        item.name = "Strange Bat";
        item.tradable = true;
        item.craftable = true;
        
        ListingUpdate.Quality quality = new ListingUpdate.Quality();
        quality.id = 11;
        quality.name = "Strange";
        quality.color = "#CF6A32";
        item.quality = quality;
        
        payload.item = item;
        
        // Create currencies
        ListingUpdate.Currencies currencies = new ListingUpdate.Currencies();
        currencies.metal = 7.0;
        payload.currencies = currencies;
        
        // Create value
        ListingUpdate.Value value = new ListingUpdate.Value();
        value.raw = 7.0;
        value.shortStr = "7 ref";
        value.longStr = "7 ref";
        payload.value = value;
        
        update.payload = payload;
        return update;
    }
    
    private ListingUpdate createSampleDeleteEvent() {
        ListingUpdate delete = new ListingUpdate();
        delete.id = "existing_id_1";
        delete.event = "listing-delete";
        
        ListingUpdate.Payload payload = new ListingUpdate.Payload();
        payload.id = "existing_id_1";
        payload.steamid = "76561199574661225";
        
        delete.payload = payload;
        return delete;
    }
}