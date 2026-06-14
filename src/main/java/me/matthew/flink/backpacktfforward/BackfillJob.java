package me.matthew.flink.backpacktfforward;

import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import me.matthew.flink.backpacktfforward.model.backfill.BackfillRequest;
import me.matthew.flink.backpacktfforward.parser.BackfillMessageParser;
import me.matthew.flink.backpacktfforward.processor.BackfillProcessor;
import me.matthew.flink.backpacktfforward.sink.ListingDeleteSink;
import me.matthew.flink.backpacktfforward.sink.ListingUpsertSink;
import me.matthew.flink.backpacktfforward.source.BackfillRequestSource;
import me.matthew.flink.backpacktfforward.source.BackfillSourceWithMetrics;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

@Slf4j
public class BackfillJob {

    public static void main(String[] args) throws Exception {
        log.info("Starting BackpackTF Backfill Job...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String dbUrl = System.getenv("DB_URL");
        String dbUser = System.getenv("DB_USERNAME");
        String dbPass = System.getenv("DB_PASSWORD");
        int upsertBatchSize = Integer.parseInt(System.getenv().getOrDefault("UPSERT_BATCH_SIZE", "10"));
        long upsertBatchIntervalMs = Long.parseLong(System.getenv().getOrDefault("UPSERT_BATCH_INTERVAL_MS", "200"));
        int deleteBatchSize = Integer.parseInt(System.getenv().getOrDefault("DELETE_BATCH_SIZE", "10"));
        long deleteBatchIntervalMs = Long.parseLong(System.getenv().getOrDefault("DELETE_BATCH_INTERVAL_MS", "1000"));

        if (dbUrl == null || dbUser == null || dbPass == null)
            throw new IllegalArgumentException("Database env vars missing");

        KafkaSource<String> backfillKafkaSource = BackfillRequestSource.createKafkaSource();

        DataStreamSource<String> backfillSource = env.fromSource(backfillKafkaSource,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "BackpackTFBackfillKafkaSource");

        var backfillRequests = backfillSource
                .flatMap(new BackfillMessageParser())
                .returns(BackfillRequest.class)
                .name("BackpackTFBackfillMessageParser");

        var backfillRequestsWithMetrics = BackfillSourceWithMetrics.addMetrics(backfillRequests)
                .name("BackpackTFBackfillSourceWithMetrics");

        // capacity=5: allows checkpoint barriers to flow through even when a request is in-flight.
        // The single-threaded executor still serializes actual API calls — extra slots queue in the executor.
        var backfillStream = AsyncDataStream.unorderedWait(
                        backfillRequestsWithMetrics,
                        new BackfillProcessor(dbUrl, dbUser, dbPass),
                        30, TimeUnit.MINUTES,
                        5)
                .returns(ListingUpdate.class)
                .name("BackpackTFBackfillProcessor");

        backfillStream
                .filter(lu -> lu != null && lu.getEvent() != null && lu.getEvent().equals("listing-update"))
                .name("BackfillListingUpdateFilter")
                .addSink(new ListingUpsertSink(dbUrl, dbUser, dbPass, upsertBatchSize, upsertBatchIntervalMs))
                .name("BackfillListingUpsertSink");

        backfillStream
                .filter(lu -> lu != null && lu.getEvent() != null && lu.getEvent().equals("listing-delete"))
                .name("BackfillListingDeleteFilter")
                .addSink(new ListingDeleteSink(dbUrl, dbUser, dbPass, deleteBatchSize, deleteBatchIntervalMs))
                .name("BackfillListingDeleteSink");

        log.info("Starting Flink backfill job execution...");
        env.execute("BackpackTF Backfill");
    }
}
