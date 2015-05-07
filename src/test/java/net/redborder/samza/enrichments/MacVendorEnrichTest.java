package net.redborder.samza.enrichments;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.CLIENT_MAC;
import static net.redborder.samza.util.constants.Dimension.CLIENT_MAC_VENDOR;

@RunWith(MockitoJUnitRunner.class)
public class MacVendorEnrichTest extends TestCase {

    @Test
    public void enrichesWithMacVendor() {
        // Enriches when the MAC is found
        MacVendorEnrich.ouiFilePath = ClassLoader.getSystemResource("mac_vendors").getFile();
        MacVendorEnrich macVendorEnrich = new MacVendorEnrich();

        Map<String, Object> messageApple = new HashMap<>();
        messageApple.put(CLIENT_MAC, "00:1C:B3:09:85:15");

        Map<String, Object> enriched = macVendorEnrich.enrich(messageApple);
        assertEquals("Apple", enriched.get(CLIENT_MAC_VENDOR));

        // It doesn't define CLIENT_MAC_VENDOR field when the MAC is not found
        Map<String, Object> messageWithoutVendor = new HashMap<>();
        messageWithoutVendor.put(CLIENT_MAC, "AA:AA:AA:AA:AA:AA");

        Map<String, Object> enrichedWithoutVendor = macVendorEnrich.enrich(messageWithoutVendor);
        assertNull(enrichedWithoutVendor.get(CLIENT_MAC_VENDOR));
    }

    @Test
    public void logsWhenVendorFileNotFound() {
        MacVendorEnrich.ouiFilePath = "/this_path_doesnt_exist";
        MacVendorEnrich macVendorEnrich = new MacVendorEnrich();
        assertTrue(macVendorEnrich.ouiMap.isEmpty());
    }
}

