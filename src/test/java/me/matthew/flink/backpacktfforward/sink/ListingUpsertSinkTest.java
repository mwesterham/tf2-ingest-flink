package me.matthew.flink.backpacktfforward.sink;

import me.matthew.flink.backpacktfforward.model.ListingUpdate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ListingUpsertSink.invalidReason, which drops rows the listings
 * table would reject so one bad listing can't fail the whole batch.
 */
public class ListingUpsertSinkTest {

    private static ListingUpdate listing(Double metal, Long keys, int defindex) {
        var item = new ListingUpdate.Item();
        item.setDefindex(defindex);
        var currencies = new ListingUpdate.Currencies();
        currencies.setMetal(metal);
        currencies.setKeys(keys);
        var payload = new ListingUpdate.Payload();
        payload.setId("440_76561198421717954_50f6aed4cbb65ce695263f21f87283b7");
        payload.setItem(item);
        payload.setCurrencies(currencies);
        var lu = new ListingUpdate();
        lu.setEvent("listing-update");
        lu.setPayload(payload);
        return lu;
    }

    @Test
    public void testValidListingAccepted() {
        assertNull(ListingUpsertSink.invalidReason(listing(1.11, 11L, 973)));
        assertNull(ListingUpsertSink.invalidReason(listing(0.0, 0L, 0)));
        assertNull(ListingUpsertSink.invalidReason(listing(null, null, 973)));
    }

    @Test
    public void testNullCurrenciesAccepted() {
        var lu = listing(1.0, 1L, 973);
        lu.getPayload().setCurrencies(null);
        assertNull(ListingUpsertSink.invalidReason(lu));
    }

    @Test
    public void testNegativeMetalRejected() {
        // Troll listing from 2026-10-20: "Buying for -0.11 ref"
        assertNotNull(ListingUpsertSink.invalidReason(listing(-0.12, null, 973)));
    }

    @Test
    public void testNegativeKeysRejected() {
        assertNotNull(ListingUpsertSink.invalidReason(listing(1.0, -1L, 973)));
    }

    @Test
    public void testMetalOverflowingHalfScrapRejected() {
        assertNotNull(ListingUpsertSink.invalidReason(listing(1.0E65, null, 973)));
        assertNotNull(ListingUpsertSink.invalidReason(listing(Double.NaN, null, 973)));
        assertNull(ListingUpsertSink.invalidReason(listing(ListingUpsertSink.MAX_METAL, null, 973)));
    }

    @Test
    public void testNegativeDefindexRejected() {
        assertNotNull(ListingUpsertSink.invalidReason(listing(1.0, null, -1)));
    }

    @Test
    public void testMissingFieldsRejected() {
        assertNotNull(ListingUpsertSink.invalidReason(null));
        assertNotNull(ListingUpsertSink.invalidReason(new ListingUpdate()));
        var noItem = listing(1.0, null, 973);
        noItem.getPayload().setItem(null);
        assertNotNull(ListingUpsertSink.invalidReason(noItem));
    }
}
