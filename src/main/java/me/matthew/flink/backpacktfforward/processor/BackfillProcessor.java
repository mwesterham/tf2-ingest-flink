package me.matthew.flink.backpacktfforward.processor;

import dev.failsafe.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.client.BackpackTfApiClient;
import me.matthew.flink.backpacktfforward.client.SteamApi;
import me.matthew.flink.backpacktfforward.config.BackpackTfApiConfiguration;
import me.matthew.flink.backpacktfforward.config.SteamApiConfiguration;
import me.matthew.flink.backpacktfforward.metrics.SqlRetryMetrics;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequestType;
import me.matthew.flink.backpacktfforward.util.DatabaseHelper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.*;

/**
 * Flink processor that handles backfill requests using a factory pattern for different request types.
 * 
 * This processor now supports multiple request types through specialized handlers:
 * - FULL: Complete backfill with all listings (existing behavior)
 * - BUY_ONLY: Only process buy listings
 * - SELL_ONLY: Only process sell listings
 * - SINGLE_ID: Process a specific listing by ID
 * - INVENTORY_FILTERED: Sell listings with Steam inventory size constraints
 * 
 * The processor uses a BackfillRequestFactory to route requests to appropriate handlers
 * while maintaining existing error handling, metrics, and performance patterns.
 * 
 * For backward compatibility, requests without explicit type specification default to FULL processing.
 */
@Slf4j
public class BackfillProcessor extends RichFlatMapFunction<BackfillRequest, ListingUpdate> {
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    
    private transient DatabaseHelper databaseHelper;
    private transient BackpackTfApiClient apiClient;
    private transient SteamApi steamApi;
    private transient BackfillRequestFactory requestFactory;
    
    // Core metrics for processor-level monitoring
    private transient Counter backfillRequestsProcessed;
    private transient Counter backfillRequestsFailed;
    
    // Performance tracking - using simple counters for latency tracking
    private volatile long lastProcessingTime = 0;
    
    /**
     * Creates a new BackfillProcessor with database connection parameters.
     * 
     * @param jdbcUrl Database JDBC URL
     * @param username Database username
     * @param password Database password
     */
    public BackfillProcessor(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Initialize metrics
        var metricGroup = getRuntimeContext().getMetricGroup();
        
        // Core processor metrics
        backfillRequestsProcessed = metricGroup.counter(BACKFILL_REQUESTS_PROCESSED);
        backfillRequestsFailed = metricGroup.counter(BACKFILL_REQUESTS_FAILED);
        
        // Performance tracking gauge
        metricGroup.gauge(BACKFILL_LAST_PROCESSING_TIME, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return lastProcessingTime;
            }
        });
        
        // Initialize client metrics
        Counter backfillApiCallsSuccess = metricGroup.counter(BACKFILL_API_CALLS_SUCCESS);
        Counter backfillApiCallsFailed = metricGroup.counter(BACKFILL_API_CALLS_FAILED);
        Counter getListingApiCallsSuccess = metricGroup.counter(GET_LISTING_API_CALLS_SUCCESS);
        Counter getListingApiCallsFailed = metricGroup.counter(GET_LISTING_API_CALLS_FAILED);
        Counter steamApiCallsSuccess = metricGroup.counter(STEAM_API_CALLS_SUCCESS);
        Counter steamApiCallsFailed = metricGroup.counter(STEAM_API_CALLS_FAILED);
        
        // Initialize retry policy using existing SqlRetryMetrics patterns
        SqlRetryMetrics sqlRetryMetrics = new SqlRetryMetrics(
                getRuntimeContext().getMetricGroup(),
                "backfill_db_retries"
        );
        RetryPolicy<Object> retryPolicy = sqlRetryMetrics.deadlockRetryPolicy(5);
        
        // Initialize database helper with existing connection patterns
        databaseHelper = new DatabaseHelper(jdbcUrl, username, password, retryPolicy);
        
        // Initialize API client with metrics
        apiClient = new BackpackTfApiClient(
                BackpackTfApiConfiguration.getApiToken(),
                backfillApiCallsSuccess,
                backfillApiCallsFailed,
                getListingApiCallsSuccess,
                getListingApiCallsFailed
        );
        
        // Initialize Steam API client with metrics
        steamApi = new SteamApi(
                SteamApiConfiguration.getSteamApiKey(),
                steamApiCallsSuccess,
                steamApiCallsFailed
        );
        
        // Initialize request factory with all handlers
        initializeRequestFactory();
        
        log.info("BackfillProcessor initialized with DatabaseHelper, BackpackTF API client, Steam API client, and request factory");
        log.info("Metrics initialized: processing_time, success/failure counters for all operations");
    }
    
    /**
     * Initializes the BackfillRequestFactory with all available handlers.
     * Each handler is configured with the required dependencies from this processor.
     */
    private void initializeRequestFactory() {
        if (databaseHelper == null || apiClient == null || steamApi == null) {
            log.warn("Cannot initialize request factory: missing dependencies (databaseHelper={}, apiClient={}, steamApi={})", 
                    databaseHelper != null, apiClient != null, steamApi != null);
            return;
        }
        
        Map<BackfillRequestType, BackfillRequestHandler> handlers = new HashMap<>();
        
        handlers.put(BackfillRequestType.FULL, new FullBackfillHandler(databaseHelper, apiClient, steamApi));
        handlers.put(BackfillRequestType.BUY_ONLY, new BuyOnlyBackfillHandler(databaseHelper, apiClient));
        handlers.put(BackfillRequestType.SELL_ONLY, new SellOnlyBackfillHandler(databaseHelper, apiClient, steamApi));
        handlers.put(BackfillRequestType.SINGLE_ID, new SingleIdBackfillHandler(databaseHelper, apiClient));
        
        requestFactory = new BackfillRequestFactory(handlers);
    }
    
    @Override
    public void flatMap(BackfillRequest request, Collector<ListingUpdate> out) throws Exception {
        long processingStartTime = System.currentTimeMillis();
        
        log.info("Starting backfill processing for item_defindex={}, item_quality_id={}, listingId={}, requestType={}", 
                request.getItemDefindex(), request.getItemQualityId(), 
                request.getListingId(), request.getRequestType());
        
        try {
            // Check if request factory is properly initialized
            if (requestFactory == null) {
                log.error("BackfillRequestFactory is not initialized. Cannot process request for item_defindex={}, item_quality_id={}, listingId={}. " +
                         "This may indicate a configuration or initialization issue.", 
                         request.getItemDefindex(), request.getItemQualityId(), request.getListingId());
                backfillRequestsFailed.inc();
                return;
            }
            
            // Use factory pattern to get appropriate handler
            BackfillRequestHandler handler = requestFactory.getHandler(request);
            
            log.debug("Selected handler: {} for request type: {}", 
                     handler.getClass().getSimpleName(), handler.getRequestType());
            
            // Delegate processing to the selected handler
            handler.process(request, out);
            
            // Mark request as successfully processed
            backfillRequestsProcessed.inc();
            
            long totalProcessingTime = System.currentTimeMillis() - processingStartTime;
            lastProcessingTime = totalProcessingTime;
            
            log.info("Backfill processing completed successfully in {}ms using {} handler for request type: {}", 
                    totalProcessingTime, handler.getClass().getSimpleName(), handler.getRequestType());
            
        } catch (IllegalArgumentException e) {
            // Handle factory/validation errors
            backfillRequestsFailed.inc();
            long totalProcessingTime = System.currentTimeMillis() - processingStartTime;
            lastProcessingTime = totalProcessingTime;
            
            log.error("Request validation failed after {}ms for item_defindex={}, item_quality_id={}, listingId={}: {}. " +
                     "Skipping this request to prevent job failure.", 
                     totalProcessingTime, request.getItemDefindex(), request.getItemQualityId(), 
                     request.getListingId(), e.getMessage());
            // Don't rethrow - continue processing other requests
            
        } catch (Exception e) {
            // Handle processing errors from handlers
            backfillRequestsFailed.inc();
            long totalProcessingTime = System.currentTimeMillis() - processingStartTime;
            lastProcessingTime = totalProcessingTime;
            
            log.error("Unexpected error processing backfill request after {}ms for item_defindex={}, item_quality_id={}, listingId={}: {}. " +
                     "This request will be skipped to prevent job failure.", 
                     totalProcessingTime, request.getItemDefindex(), request.getItemQualityId(), 
                     request.getListingId(), e.getMessage(), e);
            // Don't rethrow - continue processing other requests to maintain job stability
        }
    }
    
    @Override
    public void close() throws Exception {
        // DatabaseHelper and API clients manage their own connections, no cleanup needed
        super.close();
    }
}