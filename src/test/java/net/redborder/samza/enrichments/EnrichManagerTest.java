package net.redborder.samza.enrichments;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EnrichManagerTest extends TestCase {

    @Test
    public void enrichesCorrectly() {
        EnrichManager manager = new EnrichManager();
        Map<String, Object> message = new HashMap<>();
        Map<String, Object> macEnrichments = new HashMap<>();
        Map<String, Object> geoIpEnrichments = new HashMap<>();
        Map<String, Object> enrichedMessage = new HashMap<>();
        Map<String, Object> messageFromManager;

        // The enrichments that the mac vendor must return
        macEnrichments.put(CLIENT_MAC_VENDOR, "Apple");

        // The enrichments that the geo ip enrichment must return
        geoIpEnrichments.put(LAN_IP_COUNTRY_CODE, "ES");
        geoIpEnrichments.put(WAN_IP_COUNTRY_CODE, "US");

        // The final message that we expect from the enrich manager
        enrichedMessage.putAll(macEnrichments);
        enrichedMessage.putAll(geoIpEnrichments);

        // Mock the enrich objects to return the maps defined above
        MacVendorEnrich macVendorEnrich = mock(MacVendorEnrich.class);
        when(macVendorEnrich.enrich(message)).thenReturn(macEnrichments);

        GeoIpEnrich geoIPEnrich = mock(GeoIpEnrich.class);
        when(geoIPEnrich.enrich(message)).thenReturn(geoIpEnrichments);

        // Add the enrich objects and enrich the message
        manager.addEnrichment(macVendorEnrich);
        manager.addEnrichment(geoIPEnrich);
        messageFromManager = manager.enrich(message);

        assertEquals(enrichedMessage, messageFromManager);
    }

    @Test
    public void returnsMessageIfNotEnrichmentsApplied() {
        EnrichManager manager = new EnrichManager();
        Map<String, Object> message = new HashMap<>();
        Map<String, Object> messageFromManager;

        // Add an example dimension to the message
        message.put(CLIENT_MAC, "AA:AA:AA:AA:AA:AA");

        messageFromManager = manager.enrich(message);
        assertEquals(message, messageFromManager);
    }
}
