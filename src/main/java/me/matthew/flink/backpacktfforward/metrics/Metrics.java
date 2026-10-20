package me.matthew.flink.backpacktfforward.metrics;

public final class Metrics {

    // General application metrics
    public static final String INCOMING_EVENTS = "incoming_events";

    // Backfill NATS source message parsing metrics
    public static final String BACKFILL_MESSAGES_CONSUMED = "backfill_messages_consumed";
    public static final String BACKFILL_MESSAGES_PARSED_SUCCESS = "backfill_messages_parsed_success";
    public static final String BACKFILL_MESSAGES_PARSED_FAILED = "backfill_messages_parsed_failed";

    // NATS JetStream source metrics (Phase 3 of the Kafka -> NATS migration)
    public static final String NATS_MESSAGES_CONSUMED = "nats_messages_consumed";
    public static final String NATS_MESSAGES_PARSED_SUCCESS = "nats_messages_parsed_success";
    public static final String NATS_MESSAGES_PARSED_FAILED = "nats_messages_parsed_failed";

    // Database sink metrics
    public static final String DELETED_LISTINGS = "deleted_listings";
    public static final String DELETED_LISTINGS_RETRIES = "listing_delete_retries";
    public static final String LISTING_UPSERTS = "listing_upserts";
    public static final String LISTING_UPSERT_RETRIES = "listing_upsert_retries";
    public static final String LISTING_UPSERTS_SKIPPED_INVALID = "listing_upserts_skipped_invalid";

    // Backfill operation metrics
    public static final String BACKFILL_REQUESTS_CONSUMED = "backfill_requests_consumed";
    public static final String BACKFILL_REQUESTS_PROCESSED = "backfill_requests_processed";
    public static final String BACKFILL_REQUESTS_FAILED = "backfill_requests_failed";
    public static final String BACKFILL_API_CALLS_SUCCESS = "backfill_api_calls_success";
    public static final String BACKFILL_API_CALLS_FAILED = "backfill_api_calls_failed";

    // Steam API metrics
    public static final String STEAM_API_CALLS_SUCCESS = "steam_api_calls_success";
    public static final String STEAM_API_CALLS_FAILED = "steam_api_calls_failed";

    // BackpackTF getListing API metrics
    public static final String GET_LISTING_API_CALLS_SUCCESS = "get_listing_api_calls_success";
    public static final String GET_LISTING_API_CALLS_FAILED = "get_listing_api_calls_failed";

    // Database operation metrics
    public static final String DATABASE_QUERIES_SUCCESS = "database_queries_success";
    public static final String DATABASE_QUERIES_FAILED = "database_queries_failed";

    public static final String REALTIME_WRITES_PROCESSED = "realtime_writes_processed";

    // Performance tracking gauge metrics
    public static final String BACKFILL_LAST_API_CALL_LATENCY = "backfill_last_api_call_latency";
    public static final String BACKFILL_LAST_PROCESSING_TIME = "backfill_last_processing_time";
}
