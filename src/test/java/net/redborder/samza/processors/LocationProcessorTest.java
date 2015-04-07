package net.redborder.samza.processors;
/*
 * Copyright (c) 2015 ENEO Tecnologia S.L.
 * This file is part of redBorder.
 * redBorder is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * redBorder is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with redBorder. If not, see <http://www.gnu.org/licenses/>.
 */

import junit.framework.TestCase;
import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.MockKeyValueStore;
import net.redborder.samza.util.MockMessageCollector;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LocationProcessorTest extends TestCase {

    static MockKeyValueStore storeLocation;

    static LocationProcessor locationProcessor;
    static EnrichManager enrichManager;

    @Mock
    static StoreManager storeManager;


    @BeforeClass
    public static void initTest() {
        // This store uses an in-memory map instead of samza K/V RockDB
        storeLocation = new MockKeyValueStore();

        // Mock the storeManager in order to return the mock store
        // that we just instantiated
        storeManager = mock(StoreManager.class);
        when(storeManager.getStore(LocationV10Processor.LOCATION_STORE)).thenReturn(storeLocation);
        when(storeManager.getStore(LocationV89Processor.LOCATION_STORE)).thenReturn(storeLocation);


        enrichManager = new EnrichManager();
        locationProcessor = new LocationProcessor(storeManager, enrichManager);
    }

    @Test
    public void locationProcessorV10() {
        MockMessageCollector collector = new MockMessageCollector();
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> message = new HashMap<>();

        Map<String, Object> content = new HashMap<>();
        content.put("timestamp", "2015-03-31T02:57:38.570-0700");
        content.put("subscriptionName", "sensor-testing");
        content.put("entity", "WIRELESS_CLIENTS");
        content.put("deviceId", "00:00:00:00:00:00");
        content.put("mseUdi", "AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8");
        content.put("timestampMillis", 1427795858570L);

        Map<String, Object> location = new HashMap<>();
        location.put("macAddress", "00:00:00:00:00:00");
        location.put("ssId", "rb-corp");
        location.put("band", "UNKNOWN");
        location.put("apMacAddress", "68:bc:0c:65:0a:a0");
        location.put("dot11Status", "ASSOCIATED");
        location.put("ipAddress", (List<String>) Arrays.asList(new String[]{"10.50.22.1", "fe80:0000:0000:0000:102c:0f0e:db63:7e40"}));

        Map<String, Object> mapInfo = new HashMap<>();
        mapInfo.put("mapHierarchyString", "Campus Test>Building Test>Floor Test");

        Map<String, Object> mapCoordinate = new HashMap<>();
        mapCoordinate.put("x", 88.609215);
        mapCoordinate.put("y", 72.91531);
        mapCoordinate.put("unit", "FEET");

        location.put("MapCoordinate", mapCoordinate);
        location.put("MapInfo", mapInfo);
        content.put("location", location);
        message.put("StreamingNotification", content);

        locationProcessor.process(message, collector);

        result.put("dot11_status", "ASSOCIATED");
        result.put("bytes", 0);
        result.put("pkts", 0);
        result.put("type", "mse");
        result.put("client_campus", "Campus Test");
        result.put("client_building", "Building Test");
        result.put("timestamp", 1427795858L);
        result.put("client_mac", "00:00:00:00:00:00");
        result.put("wireless_id", "rb-corp");
        result.put("sensor_name", "sensor-testing");
        result.put("client_floor", "Floor Test");
        result.put("client_snr_num", 0);
        result.put("client_rssi", "unknown");
        result.put("client_snr", "unknown");
        result.put("client_rssi_num", 0);
        result.put("src", "10.50.22.1");
        result.put("wireless_station", "68:bc:0c:65:0a:a0");

        Map<String, Object> enrichmentMessage = collector.getResult().get(0);
        assertEquals(result, enrichmentMessage);
    }

    @Test
    public void checkName() {
        assertEquals("loc", locationProcessor.getName());
    }
}
