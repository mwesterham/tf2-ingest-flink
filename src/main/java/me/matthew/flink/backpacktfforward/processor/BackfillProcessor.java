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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.*;

/**
 * Flink async processor that handles backfill requests without blocking the task thread.
 *
 * Each request is submitted to a single-threaded executor so checkpoint barriers can
 * flow through while Steam/BackpackTF API calls are in progress. The executor is
 * single-threaded because the Steam API rate limiter is a global lock shared across
 * all handler instances — parallel submissions would just serialize there anyway.
 *
 * Capacity is set to 1 in the AsyncDataStream so Flink never queues a second request
 * before the first completes, keeping memory bounded and ordering simple.
 */
@Slf4j
public class BackfillProcessor extends RichAsyncFunction<BackfillRequest, ListingUpdate> {

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private transient DatabaseHelper databaseHelper;
    private transient BackpackTfApiClient apiClient;
    private transient SteamApi steamApi;
    private transient BackfillRequestFactory requestFactory;

    private transient Counter backfillRequestsProcessed;
    private transient Counter backfillRequestsFailed;

    private volatile long lastProcessingTime = 0;

    private transient ExecutorService executor;

    public BackfillProcessor(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        var metricGroup = getRuntimeContext().getMetricGroup();

        backfillRequestsProcessed = metricGroup.counter(BACKFILL_REQUESTS_PROCESSED);
        backfillRequestsFailed = metricGroup.counter(BACKFILL_REQUESTS_FAILED);
        metricGroup.gauge(BACKFILL_LAST_PROCESSING_TIME, (Gauge<Long>) () -> lastProcessingTime);

        Counter backfillApiCallsSuccess = metricGroup.counter(BACKFILL_API_CALLS_SUCCESS);
        Counter backfillApiCallsFailed = metricGroup.counter(BACKFILL_API_CALLS_FAILED);
        Counter getListingApiCallsSuccess = metricGroup.counter(GET_LISTING_API_CALLS_SUCCESS);
        Counter getListingApiCallsFailed = metricGroup.counter(GET_LISTING_API_CALLS_FAILED);
        Counter steamApiCallsSuccess = metricGroup.counter(STEAM_API_CALLS_SUCCESS);
        Counter steamApiCallsFailed = metricGroup.counter(STEAM_API_CALLS_FAILED);

        SqlRetryMetrics sqlRetryMetrics = new SqlRetryMetrics(
                getRuntimeContext().getMetricGroup(), "backfill_db_retries");
        RetryPolicy<Object> retryPolicy = sqlRetryMetrics.deadlockRetryPolicy(5);

        databaseHelper = new DatabaseHelper(jdbcUrl, username, password, retryPolicy);
        apiClient = new BackpackTfApiClient(
                BackpackTfApiConfiguration.getApiToken(),
                backfillApiCallsSuccess, backfillApiCallsFailed,
                getListingApiCallsSuccess, getListingApiCallsFailed);
        steamApi = new SteamApi(
                SteamApiConfiguration.getSteamApiKey(),
                steamApiCallsSuccess, steamApiCallsFailed);

        initializeRequestFactory();

        executor = Executors.newSingleThreadExecutor();

        log.info("BackfillProcessor initialized");
    }

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
    public void asyncInvoke(BackfillRequest request, ResultFuture<ListingUpdate> resultFuture) {
        executor.submit(() -> {
            long startTime = System.currentTimeMillis();
            log.info("Starting backfill processing for item_defindex={}, item_quality_id={}, listingId={}, requestType={}",
                    request.getItemDefindex(), request.getItemQualityId(),
                    request.getListingId(), request.getRequestType());
            try {
                if (requestFactory == null) {
                    log.error("BackfillRequestFactory is not initialized, skipping request for item_defindex={}",
                            request.getItemDefindex());
                    backfillRequestsFailed.inc();
                    resultFuture.complete(Collections.emptyList());
                    return;
                }

                BackfillRequestHandler handler = requestFactory.getHandler(request);

                List<ListingUpdate> results = new ArrayList<>();
                handler.process(request, new ListCollector<>(results));

                backfillRequestsProcessed.inc();
                lastProcessingTime = System.currentTimeMillis() - startTime;
                log.info("Backfill completed in {}ms using {} for requestType={}",
                        lastProcessingTime, handler.getClass().getSimpleName(), handler.getRequestType());

                resultFuture.complete(results);

            } catch (IllegalArgumentException e) {
                backfillRequestsFailed.inc();
                lastProcessingTime = System.currentTimeMillis() - startTime;
                log.error("Request validation failed after {}ms for item_defindex={}: {}",
                        lastProcessingTime, request.getItemDefindex(), e.getMessage());
                resultFuture.complete(Collections.emptyList());

            } catch (Exception e) {
                backfillRequestsFailed.inc();
                lastProcessingTime = System.currentTimeMillis() - startTime;
                log.error("Unexpected error after {}ms for item_defindex={}: {}",
                        lastProcessingTime, request.getItemDefindex(), e.getMessage(), e);
                resultFuture.complete(Collections.emptyList());
            }
        });
    }

    @Override
    public void timeout(BackfillRequest request, ResultFuture<ListingUpdate> resultFuture) {
        backfillRequestsFailed.inc();
        log.error("Backfill timed out for item_defindex={}, item_quality_id={}, requestType={}",
                request.getItemDefindex(), request.getItemQualityId(), request.getRequestType());
        resultFuture.complete(Collections.emptyList());
    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            executor.shutdown();
        }
        super.close();
    }

    /** Collector backed by a list, used to gather handler output without changing handler interfaces. */
    private static final class ListCollector<T> implements Collector<T> {
        private final List<T> list;

        ListCollector(List<T> list) {
            this.list = list;
        }

        @Override
        public void collect(T record) {
            list.add(record);
        }

        @Override
        public void close() {}
    }
}
