package me.matthew.flink.backpacktfforward.sink;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.metrics.SqlRetryMetrics;
import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static me.matthew.flink.backpacktfforward.metrics.Metrics.LISTING_UPSERTS;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.LISTING_UPSERT_RETRIES;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.LISTING_UPSERTS_SKIPPED_INVALID;
import static me.matthew.flink.backpacktfforward.metrics.Metrics.REALTIME_WRITES_PROCESSED;

@Slf4j
public class ListingUpsertSink extends RichSinkFunction<ListingUpdate> {

    /**
     * floor(Long.MAX_VALUE / 18): above this, the generated column
     * metal_half_scrap = round(metal * 18) overflows bigint.
     */
    static final double MAX_METAL = 512409557603043211.0;

    private static final String UPSERT_SQL = """ 
        INSERT INTO listings (
            id, steamid, item_defindex, item_quality_id, intent, appid, metal, keys,
            raw_value, short_value, long_value, details, listed_at,
            market_name, status, user_agent_client, user_name, user_premium, user_online,
            user_banned, user_trade_offer_url, item_tradable, item_craftable,
            item_quality_color, item_particle_name, item_particle_type, bumped_at, 
            spell_ids, strange_part_ids, paint_id, paint_name, paint_color, paint_secondary_hex, is_deleted
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, false)
        ON CONFLICT (id) DO UPDATE SET
            steamid = EXCLUDED.steamid,
            item_defindex = EXCLUDED.item_defindex,
            item_quality_id = EXCLUDED.item_quality_id,
            intent = EXCLUDED.intent,
            appid = EXCLUDED.appid,
            metal = EXCLUDED.metal,
            keys = EXCLUDED.keys,
            raw_value = EXCLUDED.raw_value,
            short_value = EXCLUDED.short_value,
            long_value = EXCLUDED.long_value,
            details = EXCLUDED.details,
            listed_at = EXCLUDED.listed_at,
            market_name = EXCLUDED.market_name,
            status = EXCLUDED.status,
            user_agent_client = EXCLUDED.user_agent_client,
            user_name = EXCLUDED.user_name,
            user_premium = EXCLUDED.user_premium,
            user_online = EXCLUDED.user_online,
            user_banned = EXCLUDED.user_banned,
            user_trade_offer_url = EXCLUDED.user_trade_offer_url,
            item_tradable = EXCLUDED.item_tradable,
            item_craftable = EXCLUDED.item_craftable,
            item_quality_color = EXCLUDED.item_quality_color,
            item_particle_name = EXCLUDED.item_particle_name,
            item_particle_type = EXCLUDED.item_particle_type,
            bumped_at = EXCLUDED.bumped_at,
            spell_ids = EXCLUDED.spell_ids,
            strange_part_ids = EXCLUDED.strange_part_ids,
            paint_id = EXCLUDED.paint_id,
            paint_name = EXCLUDED.paint_name,
            paint_color = EXCLUDED.paint_color,
            paint_secondary_hex = EXCLUDED.paint_secondary_hex,
            is_deleted = false;
        """;

    private Connection connection;
    private PreparedStatement stmt;
    private transient Counter upsertCounter;
    private transient Counter realTimeWritesCounter;
    private transient Counter skippedInvalidCounter;

    private final List<ListingUpdate> batch = new ArrayList<>();
    private final int batchSize;
    private final long batchIntervalMs;
    private long lastFlushTime;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private RetryPolicy<Object> retryPolicy;

    public ListingUpsertSink(String jdbcUrl, String username, String password, int batchSize, long batchIntervalMs) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false);
        stmt = connection.prepareStatement(UPSERT_SQL);

        upsertCounter = getRuntimeContext().getMetricGroup().counter(LISTING_UPSERTS);
        realTimeWritesCounter = getRuntimeContext().getMetricGroup().counter(REALTIME_WRITES_PROCESSED);
        skippedInvalidCounter = getRuntimeContext().getMetricGroup().counter(LISTING_UPSERTS_SKIPPED_INVALID);
        
        SqlRetryMetrics sqlRetryMetrics = new SqlRetryMetrics(
                getRuntimeContext().getMetricGroup(),
                LISTING_UPSERT_RETRIES
        );
        lastFlushTime = System.currentTimeMillis();

        retryPolicy = sqlRetryMetrics.deadlockRetryPolicy(5);
    }

    @Override
    public void invoke(ListingUpdate lu, Context context) throws Exception {
        // Drop rows the listings table would reject (troll prices etc.). One bad
        // row fails the whole batch, and since the batch is never cleared on
        // failure it would poison every flush after it.
        String invalidReason = invalidReason(lu);
        if (invalidReason != null) {
            skippedInvalidCounter.inc();
            log.warn("Skipping invalid listing update {}: {}",
                    lu != null && lu.getPayload() != null ? lu.getPayload().getId() : null, invalidReason);
            return;
        }

        // Track real-time vs backfill writes separately for monitoring
        if (lu.getGenerationTimestamp() == null) {
            realTimeWritesCounter.inc();
            batch.add(lu);
        }
        else { // this is a backfill request
            batch.add(lu);
        }

        long now = System.currentTimeMillis();
        if (batch.size() >= batchSize || now - lastFlushTime >= batchIntervalMs) {
            flushBatch();
            lastFlushTime = now;
        }
    }

    /**
     * Flush with deterministic ordering + Failsafe retry
     */
    private void flushBatch() throws SQLException {
        if (batch.isEmpty())
            return;

        // Sort rows by PK to guarantee consistent lock order
        batch.sort(Comparator.comparing(l -> l.getPayload().getId()));

        Failsafe.with(retryPolicy).run(() -> {
            try {
                stmt.clearBatch();

                for (ListingUpdate lu : batch) {
                    var p = lu.getPayload();

                    stmt.setString(1, p.getId());
                    stmt.setString(2, p.getSteamid());
                    stmt.setInt(3, p.getItem().getDefindex());
                    stmt.setObject(4, p.getItem().getQuality() != null ? p.getItem().getQuality().getId() : null, Types.INTEGER);
                    stmt.setString(5, p.getIntent());
                    stmt.setInt(6, p.getAppid());

                    if (p.getCurrencies() != null && p.getCurrencies().getMetal() != null)
                        stmt.setDouble(7, p.getCurrencies().getMetal());
                    else stmt.setNull(7, Types.DOUBLE);

                    if (p.getCurrencies() != null && p.getCurrencies().getKeys() != null)
                        stmt.setLong(8, p.getCurrencies().getKeys());
                    else stmt.setNull(8, Types.BIGINT);

                    if (p.getValue() != null)
                        stmt.setDouble(9, p.getValue().getRaw());
                    else stmt.setNull(9, Types.DOUBLE);

                    stmt.setString(10, p.getValue() != null ? p.getValue().getShortStr() : null);
                    stmt.setString(11, p.getValue() != null ? p.getValue().getLongStr() : null);
                    stmt.setString(12, p.getDetails());
                    stmt.setLong(13, p.getListedAt() * 1000);
                    stmt.setString(14, p.getItem() != null ? p.getItem().getMarketName() : null);
                    stmt.setString(15, p.getStatus());
                    stmt.setString(16, p.getUserAgent() != null ? p.getUserAgent().getClient() : null);
                    stmt.setString(17, p.getUser() != null ? p.getUser().getName() : null);

                    stmt.setObject(18, p.getUser() != null ? p.getUser().getPremium() : null, Types.BOOLEAN);
                    stmt.setObject(19, p.getUser() != null ? p.getUser().getOnline() : null, Types.BOOLEAN);
                    stmt.setObject(20, p.getUser() != null ? p.getUser().getBanned() : null, Types.BOOLEAN);
                    stmt.setString(21, p.getUser() != null ? p.getUser().getTradeOfferUrl() : null);

                    stmt.setObject(22, p.getItem() != null ? p.getItem().getTradable() : null, Types.BOOLEAN);
                    stmt.setObject(23, p.getItem() != null ? p.getItem().getCraftable() : null, Types.BOOLEAN);
                    stmt.setString(24, p.getItem() != null && p.getItem().getQuality() != null ? p.getItem().getQuality().getColor() : null);
                    stmt.setString(25, p.getItem() != null && p.getItem().getParticle() != null ? p.getItem().getParticle().getName() : null);
                    stmt.setString(26, p.getItem() != null && p.getItem().getParticle() != null ? p.getItem().getParticle().getType() : null);
                    stmt.setLong(27, p.getBumpedAt() * 1000);
                    
                    // Set spell IDs array
                    stmt.setArray(28, createSpellIdsArray(p, connection));
                    
                    // Set strange part IDs array
                    stmt.setArray(29, createStrangePartIdsArray(p, connection));
                    
                    // Set paint fields
                    if (p.getItem() != null && p.getItem().getPaint() != null) {
                        stmt.setInt(30, p.getItem().getPaint().getId());
                        stmt.setString(31, p.getItem().getPaint().getName());
                        stmt.setString(32, p.getItem().getPaint().getColor());
                    } else {
                        stmt.setNull(30, Types.INTEGER);
                        stmt.setNull(31, Types.VARCHAR);
                        stmt.setNull(32, Types.VARCHAR);
                    }
                    
                    // Set paint secondary hex
                    stmt.setString(33, p.getItem() != null ? p.getItem().getPaintSecondaryHex() : null);

                    stmt.addBatch();
                }

                stmt.executeBatch();
                connection.commit();
                // Count only committed rows, not retry attempts
                upsertCounter.inc(batch.size());
            }
            catch (SQLException ex) {
                connection.rollback();
                throw ex;
            }
        });

        batch.clear();
    }

    /**
     * Returns why the update can't be written to the listings table, or null if it can.
     * Mirrors the table's CHECK constraints and the metal_half_scrap bigint range.
     */
    static String invalidReason(ListingUpdate lu) {
        if (lu == null || lu.getPayload() == null)
            return "missing payload";
        var p = lu.getPayload();
        if (p.getId() == null)
            return "missing id";
        if (p.getItem() == null)
            return "missing item";
        if (p.getItem().getDefindex() < 0)
            return "negative defindex " + p.getItem().getDefindex();
        if (p.getCurrencies() != null) {
            Double metal = p.getCurrencies().getMetal();
            if (metal != null && (metal.isNaN() || metal < 0 || metal > MAX_METAL))
                return "metal out of range " + metal;
            Long keys = p.getCurrencies().getKeys();
            if (keys != null && keys < 0)
                return "negative keys " + keys;
        }
        return null;
    }

    /**
     * Creates a PostgreSQL array of spell IDs from the ListingUpdate payload.
     * Returns null if no spells are present.
     */
    private Array createSpellIdsArray(ListingUpdate.Payload payload, Connection connection) throws SQLException {
        if (payload.getItem() == null || payload.getItem().getSpells() == null || payload.getItem().getSpells().isEmpty()) {
            return null;
        }
        
        String[] spellIds = payload.getItem().getSpells().stream()
                .map(spell -> spell.getId())
                .filter(id -> id != null && !id.trim().isEmpty())
                .toArray(String[]::new);
                
        return spellIds.length > 0 ? connection.createArrayOf("text", spellIds) : null;
    }

    /**
     * Creates a PostgreSQL array of strange part kill eater IDs from the ListingUpdate payload.
     * Returns null if no strange parts are present.
     */
    private Array createStrangePartIdsArray(ListingUpdate.Payload payload, Connection connection) throws SQLException {
        if (payload.getItem() == null || payload.getItem().getStrangeParts() == null || payload.getItem().getStrangeParts().isEmpty()) {
            return null;
        }
        
        String[] strangePartIds = payload.getItem().getStrangeParts().stream()
                .map(part -> part.getKillEater() != null ? String.valueOf(part.getKillEater().getId()) : null)
                .filter(id -> id != null && !id.trim().isEmpty())
                .toArray(String[]::new);
                
        return strangePartIds.length > 0 ? connection.createArrayOf("text", strangePartIds) : null;
    }

    @Override
    public void close() throws Exception {
        flushBatch();
        if (stmt != null) stmt.close();
        if (connection != null) connection.close();
        super.close();
    }
}
