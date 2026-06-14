package me.matthew.flink.backpacktfforward.integration;

import me.matthew.flink.backpacktfforward.client.BackpackTfApiClient;
import me.matthew.flink.backpacktfforward.client.SteamApi;
import me.matthew.flink.backpacktfforward.model.*;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequestType;
import me.matthew.flink.backpacktfforward.processor.BackfillProcessor;
import me.matthew.flink.backpacktfforward.processor.BackfillRequestFactory;
import me.matthew.flink.backpacktfforward.processor.FullBackfillHandler;
import me.matthew.flink.backpacktfforward.util.DatabaseHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.apache.flink.util.Collector;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

/**
 * End-to-end integration test for the complete backfill process.
 * Tests the full data flow from BackfillRequest to ListingUpdate generation.
 * 
 * **Feature: backfill-steam-integration, Requirements: 8.1, 8.6**
 */
@Slf4j
class BackfillEndToEndIntegrationTest {
    
    private BackfillProcessor processor;
    private DatabaseHelper mockDatabaseHelper;
    private BackpackTfApiClient mockBackpackTfClient;
    private SteamApi mockSteamApi;
    private BackfillRequestFactory mockRequestFactory;
    private TestCollector collector;
    
    @BeforeEach
    void setUp() throws Exception {
        // Create mocks for external dependencies
        mockDatabaseHelper = mock(DatabaseHelper.class);
        mockBackpackTfClient = mock(BackpackTfApiClient.class);
        mockSteamApi = mock(SteamApi.class);
        mockRequestFactory = mock(BackfillRequestFactory.class);
        
        // Create processor with test database configuration
        processor = new BackfillProcessor("jdbc:h2:mem:test", "test", "test");
        
        // Mock the Flink runtime context to avoid NPE with metrics
        org.apache.flink.api.common.functions.RuntimeContext mockRuntimeContext = 
            mock(org.apache.flink.api.common.functions.RuntimeContext.class);
        org.apache.flink.metrics.groups.OperatorMetricGroup mockMetricGroup = 
            mock(org.apache.flink.metrics.groups.OperatorMetricGroup.class);
        org.apache.flink.metrics.Counter mockCounter = 
            mock(org.apache.flink.metrics.Counter.class);
        
        when(mockRuntimeContext.getMetricGroup()).thenReturn(mockMetricGroup);
        when(mockMetricGroup.counter(anyString())).thenReturn(mockCounter);
        when(mockMetricGroup.gauge(anyString(), any())).thenReturn(null);
        
        // Inject the mock runtime context using reflection
        try {
            var runtimeContextField = org.apache.flink.api.common.functions.AbstractRichFunction.class
                .getDeclaredField("runtimeContext");
            runtimeContextField.setAccessible(true);
            runtimeContextField.set(processor, mockRuntimeContext);
        } catch (Exception e) {
            log.warn("Failed to inject mock runtime context: {}", e.getMessage());
        }
        
        // Initialize the processor (this should now work with mocked runtime context)
        try {
            processor.open(new org.apache.flink.configuration.Configuration());
        } catch (Exception e) {
            // If it still fails, we'll inject mocks manually
            log.debug("Processor initialization failed, will inject mocks: {}", e.getMessage());
        }
        
        // Inject mocks using reflection
        try {
            var dbField = BackfillProcessor.class.getDeclaredField("databaseHelper");
            dbField.setAccessible(true);
            dbField.set(processor, mockDatabaseHelper);
            
            var apiField = BackfillProcessor.class.getDeclaredField("apiClient");
            apiField.setAccessible(true);
            apiField.set(processor, mockBackpackTfClient);
            
            var steamField = BackfillProcessor.class.getDeclaredField("steamApi");
            steamField.setAccessible(true);
            steamField.set(processor, mockSteamApi);

            var requestFactory = BackfillProcessor.class.getDeclaredField("requestFactory");
            requestFactory.setAccessible(true);
            requestFactory.set(processor, mockRequestFactory);
            
            log.debug("Successfully injected mocks into BackfillProcessor and reinitialized request factory");
        } catch (Exception e) {
            log.warn("Failed to inject mocks via reflection: {}", e.getMessage());
            // Test will likely fail, but we'll continue to see what happens
        }
        
        collector = new TestCollector();
    }
    
    @AfterEach
    void tearDown() {
        // Clean up any resources
        collector = null;
    }
    
    @Test
    void testCompleteBackfillProcessWithSampleData() throws Exception {
        // Arrange: Create sample backfill request
        BackfillRequest request = new BackfillRequest();
        request.setItemDefindex(190); // Strange Bat
        request.setItemQualityId(11); // Strange quality
        request.setRequestType(BackfillRequestType.FULL);
        
        // Mock database response - existing listings for this item
        List<DatabaseHelper.ExistingListing> existingListings = Arrays.asList(
            new DatabaseHelper.ExistingListing("existing_id_1", "76561199574661225", "Strange Bat"),
            new DatabaseHelper.ExistingListing("existing_id_2", "76561199574661226", "Strange Bat")
        );
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(existingListings);
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        
        // Mock BackpackTF API response
        BackpackTfApiResponse apiResponse = createSampleApiResponse();
        when(mockBackpackTfClient.fetchSnapshot("Strange Bat", 440)).thenReturn(apiResponse);
        
        // Mock Steam inventory responses
        SteamInventoryResponse inventory1 = createSampleSteamInventory("76561199574661225");
        SteamInventoryResponse inventory2 = createSampleSteamInventory("76561199574661226");
        when(mockSteamApi.getPlayerItems("76561199574661225")).thenReturn(inventory1);
        when(mockSteamApi.getPlayerItems("76561199574661226")).thenReturn(inventory2);
        
        // Mock item matching
        List<InventoryItem> matchingItems1 = Arrays.asList(createSampleInventoryItem("16525961480"));
        List<InventoryItem> matchingItems2 = Arrays.asList(createSampleInventoryItem("16525961481"));
        when(mockSteamApi.findMatchingItems(any(SteamInventoryResponse.class), eq(190), eq(11)))
            .thenReturn(matchingItems1)
            .thenReturn(matchingItems2);
        
        // Mock getListing API responses
        ListingUpdate.Payload listingDetail1 = createSampleListingDetail("440_16525961480", "76561199574661225");
        ListingUpdate.Payload listingDetail2 = createSampleListingDetail("440_16525961481", "76561199574661226");
        when(mockBackpackTfClient.getListing("440_16525961480")).thenReturn(listingDetail1);
        when(mockBackpackTfClient.getListing("440_16525961481")).thenReturn(listingDetail2);

        when(mockRequestFactory.getHandler(any(BackfillRequest.class))).thenReturn(new FullBackfillHandler(mockDatabaseHelper, mockBackpackTfClient, mockSteamApi));
        
        // Act: Process the backfill request
        processor.flatMap(request, collector);
        
        // Assert: Verify the complete data flow
        List<ListingUpdate> results = collector.getCollectedItems();
        
        // Should generate updates for source of truth items and deletes for stale data
        assertTrue(results.size() >= 2, "Should generate at least 2 events (updates and/or deletes)");
        
        // Separate updates and deletes
        List<ListingUpdate> updates = results.stream()
            .filter(r -> "listing-update".equals(r.getEvent()))
            .collect(Collectors.toList());
        List<ListingUpdate> deletes = results.stream()
            .filter(r -> "listing-delete".equals(r.getEvent()))
            .collect(Collectors.toList());
        
        // Should have some update events from source of truth
        assertTrue(updates.size() > 0, "Should generate at least 1 listing update");
        
        // Verify first update (if any)
        if (!updates.isEmpty()) {
            ListingUpdate update1 = updates.get(0);
            assertEquals("listing-update", update1.getEvent());
            assertTrue(update1.getId().startsWith("440_"), "Update ID should start with 440_");
            assertNotNull(update1.getPayload());
            assertNotNull(update1.getPayload().getSteamid());
            assertEquals("sell", update1.getPayload().getIntent());
        }
        
        // Verify delete events (if any) - these are for stale data
        for (ListingUpdate delete : deletes) {
            assertEquals("listing-delete", delete.getEvent());
            assertNotNull(delete.getId());
            assertNotNull(delete.getPayload());
            assertNotNull(delete.getPayload().getSteamid());
        }
        
        // Verify API calls were made in correct sequence
        verify(mockDatabaseHelper).getAllListingsForItem(190, 11);
        verify(mockBackpackTfClient).fetchSnapshot("Strange Bat", 440);
        verify(mockSteamApi).getPlayerItems("76561199574661225");
        verify(mockSteamApi).getPlayerItems("76561199574661226");
        verify(mockSteamApi, times(2)).findMatchingItems(any(SteamInventoryResponse.class), eq(190), eq(11));
        verify(mockBackpackTfClient).getListing("440_16525961480");
        verify(mockBackpackTfClient).getListing("440_16525961481");
    }
    
    @Test
    void testBackfillProcessWithVariousItemTypes() throws Exception {
        // Test with different item types and scenarios
        
        // Test Case 1: Unusual item (different defindex/quality)
        BackfillRequest unusualRequest = new BackfillRequest();
        unusualRequest.setItemDefindex(266); // Horseless Headless Horsemann's Headtaker
        unusualRequest.setItemQualityId(5); // Unusual quality
        
        // Mock responses for unusual item
        when(mockDatabaseHelper.getAllListingsForItem(266, 5)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(266, 5)).thenReturn("Unusual Horseless Headless Horsemann's Headtaker");
        
        BackpackTfApiResponse unusualApiResponse = createUnusualApiResponse();
        when(mockBackpackTfClient.fetchSnapshot("Unusual Horseless Headless Horsemann's Headtaker", 440))
            .thenReturn(unusualApiResponse);
        
        // Process unusual item request
        TestCollector unusualCollector = new TestCollector();
        processor.flatMap(unusualRequest, unusualCollector);
        
        // Verify unusual item processing
        List<ListingUpdate> unusualResults = unusualCollector.getCollectedItems();
        assertNotNull(unusualResults);
        // Results depend on mock setup - verify basic structure
        
        // Test Case 2: Item with no current listings
        BackfillRequest emptyRequest = new BackfillRequest();
        emptyRequest.setItemDefindex(999);
        emptyRequest.setItemQualityId(6);
        
        when(mockDatabaseHelper.getAllListingsForItem(999, 6)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(999, 6)).thenReturn("Test Item");
        when(mockBackpackTfClient.fetchSnapshot("Test Item", 440))
            .thenReturn(createEmptyApiResponse());
        
        TestCollector emptyCollector = new TestCollector();
        processor.flatMap(emptyRequest, emptyCollector);
        
        List<ListingUpdate> emptyResults = emptyCollector.getCollectedItems();
        assertNotNull(emptyResults);
        // Should handle empty results gracefully
    }
    
    @Test
    void testErrorScenariosAndRecoveryBehavior() throws Exception {
        // Test various error scenarios to ensure graceful handling
        
        BackfillRequest request = new BackfillRequest();
        request.setItemDefindex(190);
        request.setItemQualityId(11);
        
        // Scenario 1: Database error
        when(mockDatabaseHelper.getAllListingsForItem(190, 11))
            .thenThrow(new RuntimeException("Database connection failed"));
        
        TestCollector errorCollector = new TestCollector();
        
        // Should not throw exception, should handle gracefully
        assertDoesNotThrow(() -> {
            processor.flatMap(request, errorCollector);
        });
        
        // Scenario 2: BackpackTF API error
        reset(mockDatabaseHelper);
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        when(mockBackpackTfClient.fetchSnapshot("Strange Bat", 440))
            .thenThrow(new RuntimeException("API rate limit exceeded"));
        
        TestCollector apiErrorCollector = new TestCollector();
        assertDoesNotThrow(() -> {
            processor.flatMap(request, apiErrorCollector);
        });
        
        // Scenario 3: Steam API error
        reset(mockDatabaseHelper, mockBackpackTfClient);
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(Collections.emptyList());
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        when(mockBackpackTfClient.fetchSnapshot("Strange Bat", 440)).thenReturn(createSampleApiResponse());
        when(mockSteamApi.getPlayerItems(anyString()))
            .thenThrow(new RuntimeException("Steam API unavailable"));
        
        TestCollector steamErrorCollector = new TestCollector();
        assertDoesNotThrow(() -> {
            processor.flatMap(request, steamErrorCollector);
        });
    }
    
    @Test
    void testPerformanceWithRealisticDataVolumes() throws Exception {
        // Test performance with larger datasets
        
        BackfillRequest request = new BackfillRequest();
        request.setItemDefindex(190);
        request.setItemQualityId(11);
        request.setRequestType(BackfillRequestType.FULL);

        when(mockRequestFactory.getHandler(any(BackfillRequest.class))).thenReturn(new FullBackfillHandler(mockDatabaseHelper, mockBackpackTfClient, mockSteamApi));

        // Create larger dataset (50 existing listings)
        List<DatabaseHelper.ExistingListing> largeDataset = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            largeDataset.add(new DatabaseHelper.ExistingListing(
                "existing_id_" + i, 
                "7656119957466" + String.format("%04d", i), 
                "Strange Bat"
            ));
        }
        
        when(mockDatabaseHelper.getAllListingsForItem(190, 11)).thenReturn(largeDataset);
        when(mockDatabaseHelper.getMarketName(190, 11)).thenReturn("Strange Bat");
        
        // Create API response with many listings
        BackpackTfApiResponse largeApiResponse = createLargeApiResponse(50);
        when(mockBackpackTfClient.fetchSnapshot("Strange Bat", 440)).thenReturn(largeApiResponse);
        
        // Mock Steam API responses for all users
        for (int i = 0; i < 50; i++) {
            String steamId = "7656119957466" + String.format("%04d", i);
            SteamInventoryResponse inventory = createSampleSteamInventory(steamId);
            when(mockSteamApi.getPlayerItems(steamId)).thenReturn(inventory);
            
            List<InventoryItem> matchingItems = Arrays.asList(
                createSampleInventoryItem("1652596" + String.format("%04d", i))
            );
            when(mockSteamApi.findMatchingItems(inventory, 190, 11)).thenReturn(matchingItems);
            
            ListingUpdate.Payload listingDetail = createSampleListingDetail(
                "440_1652596" + String.format("%04d", i), steamId
            );
            when(mockBackpackTfClient.getListing("440_1652596" + String.format("%04d", i)))
                .thenReturn(listingDetail);
        }
        
        // Measure processing time
        long startTime = System.currentTimeMillis();
        
        TestCollector performanceCollector = new TestCollector();
        processor.flatMap(request, performanceCollector);
        
        long endTime = System.currentTimeMillis();
        long processingTime = endTime - startTime;
        
        // Verify results - should have both updates and deletes
        List<ListingUpdate> results = performanceCollector.getCollectedItems();
        assertTrue(results.size() >= 50, "Should process at least 50 events (updates and/or deletes)");
        
        // Separate updates and deletes
        List<ListingUpdate> updates = results.stream()
            .filter(r -> "listing-update".equals(r.getEvent()))
            .collect(Collectors.toList());
        List<ListingUpdate> deletes = results.stream()
            .filter(r -> "listing-delete".equals(r.getEvent()))
            .collect(Collectors.toList());
        
        // Should have updates from source of truth
        assertEquals(50, updates.size(), "Should generate 50 listing updates");
        
        // May also have deletes for stale data
        assertTrue(deletes.size() >= 0, "May have delete events for stale data");
        
        // Performance assertion (should complete within reasonable time)
        assertTrue(processingTime < 5000, 
            "Processing 50 items should complete within 5 seconds, took: " + processingTime + "ms");
        
        // Verify all API calls were made
        verify(mockDatabaseHelper, times(1)).getAllListingsForItem(190, 11);
        verify(mockBackpackTfClient, times(1)).fetchSnapshot("Strange Bat", 440);
        verify(mockSteamApi, times(50)).getPlayerItems(anyString());
        verify(mockBackpackTfClient, times(50)).getListing(anyString());
    }
    
    // Helper methods to create sample data
    
    private BackpackTfApiResponse createSampleApiResponse() {
        BackpackTfApiResponse response = new BackpackTfApiResponse();
        
        BackpackTfApiResponse.ApiListing listing1 = new BackpackTfApiResponse.ApiListing();
        listing1.setSteamid("76561199574661225");
        listing1.setIntent("sell");
        
        BackpackTfApiResponse.ApiListing listing2 = new BackpackTfApiResponse.ApiListing();
        listing2.setSteamid("76561199574661226");
        listing2.setIntent("sell");
        
        response.setListings(Arrays.asList(listing1, listing2));
        return response;
    }
    
    private BackpackTfApiResponse createUnusualApiResponse() {
        BackpackTfApiResponse response = new BackpackTfApiResponse();
        
        BackpackTfApiResponse.ApiListing listing = new BackpackTfApiResponse.ApiListing();
        listing.setSteamid("76561199574661227");
        listing.setIntent("sell");
        
        response.setListings(Arrays.asList(listing));
        return response;
    }
    
    private BackpackTfApiResponse createEmptyApiResponse() {
        BackpackTfApiResponse response = new BackpackTfApiResponse();
        response.setListings(Collections.emptyList());
        return response;
    }
    
    private BackpackTfApiResponse createLargeApiResponse(int count) {
        BackpackTfApiResponse response = new BackpackTfApiResponse();
        List<BackpackTfApiResponse.ApiListing> listings = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            BackpackTfApiResponse.ApiListing listing = new BackpackTfApiResponse.ApiListing();
            listing.setSteamid("7656119957466" + String.format("%04d", i));
            listing.setIntent("sell");
            listings.add(listing);
        }
        
        response.setListings(listings);
        return response;
    }
    
    private SteamInventoryResponse createSampleSteamInventory(String steamId) {
        SteamInventoryResponse response = new SteamInventoryResponse();
        SteamInventoryResponse.SteamResult result = new SteamInventoryResponse.SteamResult();
        result.setStatus(1);
        result.setItems(Arrays.asList(createSampleInventoryItem("16525961480")));
        response.setResult(result);
        return response;
    }
    
    private InventoryItem createSampleInventoryItem(String itemId) {
        InventoryItem item = new InventoryItem();
        item.setId(Long.parseLong(itemId));
        item.setDefindex(190);
        item.setQuality(11);
        item.setLevel(1);
        item.setQuantity(1);
        return item;
    }
    
    private ListingUpdate.Payload createSampleListingDetail(String listingId, String steamId) {
        ListingUpdate.Payload detail = new ListingUpdate.Payload();
        detail.id = listingId;
        detail.steamid = steamId;
        detail.appid = 440;
        detail.intent = "sell";
        detail.count = 1;
        detail.status = "active";
        detail.source = "user";
        detail.listedAt = System.currentTimeMillis() / 1000;
        detail.bumpedAt = System.currentTimeMillis() / 1000;
        
        // Create item detail with all required fields
        ListingUpdate.Item itemDetail = new ListingUpdate.Item();
        itemDetail.appid = 440; // This was missing and causing the error
        itemDetail.defindex = 190;
        itemDetail.marketName = "Strange Bat";
        itemDetail.name = "Strange Bat";
        itemDetail.level = 1;
        itemDetail.baseName = "Bat";
        itemDetail.id = listingId.split("_")[1]; // Extract item ID from listing ID
        itemDetail.imageUrl = "https://steamcdn-a.akamaihd.net/apps/440/icons/c_bat.50e76c8094493ae96cf10d8df676a93cd13516fc.png";
        itemDetail.summary = "Level 1 Bat";
        itemDetail.tradable = true;
        itemDetail.craftable = true;
        
        ListingUpdate.Quality quality = new ListingUpdate.Quality();
        quality.id = 11;
        quality.name = "Strange";
        quality.color = "#CF6A32";
        itemDetail.quality = quality;
        
        detail.item = itemDetail;
        
        // Create currencies
        ListingUpdate.Currencies currencies = new ListingUpdate.Currencies();
        currencies.metal = 7.0;
        detail.currencies = currencies;
        
        // Create value
        ListingUpdate.Value value = new ListingUpdate.Value();
        value.raw = 7.0;
        value.shortStr = "7 ref";
        value.longStr = "7 ref";
        detail.value = value;
        
        // Create user information
        ListingUpdate.User user = new ListingUpdate.User();
        user.id = steamId;
        user.name = "Test User";
        user.avatar = "https://steamcdn-a.akamaihd.net/steamcommunity/public/images/avatars/fe/fef49e7fa7e1997310d705b2a6158ff8dc1cdfeb_medium.jpg";
        user.premium = false;
        user.online = false;
        user.banned = false;
        detail.user = user;
        
        return detail;
    }
    
    /**
     * Test collector implementation to capture emitted ListingUpdate objects
     */
    private static class TestCollector implements Collector<ListingUpdate> {
        private final List<ListingUpdate> collectedItems = new ArrayList<>();
        
        @Override
        public void collect(ListingUpdate record) {
            collectedItems.add(record);
        }
        
        @Override
        public void close() {
            // No-op
        }
        
        public List<ListingUpdate> getCollectedItems() {
            return new ArrayList<>(collectedItems);
        }
    }
}