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
import net.redborder.samza.util.MockTaskContext;
import net.redborder.samza.util.constants.Dimension;
import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;

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

    @Mock
    static Config config;

    static TaskContext taskContext;


    @BeforeClass
    public static void initTest() {
        // This store uses an in-memory map instead of samza K/V RockDB
        storeLocation = new MockKeyValueStore();
        config = mock(Config.class);
        taskContext = new MockTaskContext();

        // Mock the storeManager in order to return the mock store
        // that we just instantiated
        storeManager = mock(StoreManager.class);
        when(storeManager.getStore(LocationV10Processor.LOCATION_STORE)).thenReturn(storeLocation);
        when(storeManager.getStore(LocationV89Processor.LOCATION_STORE)).thenReturn(storeLocation);


        enrichManager = new EnrichManager();

        locationProcessor = new LocationProcessor(storeManager, enrichManager, config, taskContext);
    }

    @Test
    public void locationProcessorV89() {
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
    public void locationProcessorV10() {
        MockMessageCollector collector = new MockMessageCollector();
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> contentsLoc = new ArrayList<>();
        Map<String, Object> contentLoc = new HashMap<>();
        Map<String, Object> enrichmentMessage;

        Map<String, Object> messageLocUp = new HashMap<>();
        contentLoc.put(Dimension.LOC_NOTIFICATION_TYPE, "locationupdate");
        contentLoc.put(Dimension.LOC_SUBSCRIPTION_NAME, "rb-loc");
        contentLoc.put(Dimension.LOC_DEVICEID, "00:00:00:00:00:00");
        contentLoc.put("entity", "WIRELESS_CLIENTS");
        contentLoc.put(Dimension.LOC_SSID, "rb-corp");
        contentLoc.put(Dimension.LOC_BAND, null);
        contentLoc.put(Dimension.LOC_AP_MACADDR, "AA:AA:AA:AA:AA:AA");
        contentLoc.put(Dimension.LOC_MAP_HIERARCHY_V10, "CampusA>BuildingB>FloorC>ZoneD");
        contentLoc.put(Dimension.TIMESTAMP, 1424767310026L);
        Map<String, Object> coordinate = new HashMap<>();
        coordinate.put("x", 171.231);
        coordinate.put("y", 1.241);
        coordinate.put("z", 0.0);
        contentLoc.put("locationCoordinate", coordinate);
        contentsLoc.add(contentLoc);
        messageLocUp.put("notifications", contentsLoc);

        locationProcessor.process(messageLocUp, collector);

        result.put(Dimension.TIMESTAMP, 1424767310L);
        result.put(Dimension.CLIENT_ZONE, "ZoneD");
        result.put(Dimension.CLIENT_FLOOR, "FloorC");
        result.put(Dimension.CLIENT_BUILDING, "BuildingB");
        result.put(Dimension.CLIENT_CAMPUS, "CampusA");
        result.put(Dimension.CLIENT_FLOOR, "FloorC");
        result.put(Dimension.CLIENT_MAC, "00:00:00:00:00:00");
        result.put(Dimension.DOT11STATUS, "PROBING");
        result.put(Dimension.WIRELESS_STATION, "AA:AA:AA:AA:AA:AA");
        result.put(Dimension.PKTS, 0);
        result.put(Dimension.BYTES, 0);
        result.put(Dimension.TYPE, "mse10");
        result.put(Dimension.SENSOR_NAME, "rb-loc");

        enrichmentMessage = collector.getResult().get(0);
        assertEquals(result, enrichmentMessage);

        result.clear();

        List<Map<String, Object>> contentsAssoc = new ArrayList<>();
        Map<String, Object> contentAssoc = new HashMap<>();

        Map<String, Object> messageAssoc = new HashMap<>();
        contentAssoc.put(Dimension.LOC_NOTIFICATION_TYPE, "association");
        contentAssoc.put(Dimension.LOC_SUBSCRIPTION_NAME, "rb-assoc");
        contentAssoc.put(Dimension.LOC_DEVICEID, "00:00:00:00:00:00");
        contentAssoc.put("entity", "WIRELESS_CLIENTS");
        contentAssoc.put(Dimension.LOC_SSID, "rb-corp");
        contentAssoc.put(Dimension.LOC_BAND, "IEEE_802_11_B");
        contentAssoc.put(Dimension.LOC_AP_MACADDR, "AA:AA:AA:AA:AA:AA");
        contentAssoc.put(Dimension.LOC_IPADDR, Arrays.asList(new String[]{"25.145.34.131"}));
        contentAssoc.put(Dimension.LOC_STATUS, 3);
        contentAssoc.put(Dimension.LOC_USERNAME, "");
        contentAssoc.put(Dimension.TIMESTAMP, 1424767310026L);
        contentsAssoc.add(contentAssoc);
        messageAssoc.put("notifications", contentsAssoc);

        locationProcessor.process(messageAssoc, collector);

        result.put(Dimension.TIMESTAMP, 1424767310L);
        result.put(Dimension.CLIENT_MAC, "00:00:00:00:00:00");
        result.put(Dimension.WIRELESS_ID, "rb-corp");
        result.put(Dimension.DOT11STATUS, "ASSOCIATED");
        result.put(Dimension.PKTS, 0);
        result.put(Dimension.BYTES, 0);
        result.put(Dimension.SENSOR_NAME, "rb-assoc");
        result.put(Dimension.WIRELESS_STATION, "AA:AA:AA:AA:AA:AA");
        result.put(Dimension.DOT11PROTOCOL, "IEEE_802_11_B");
        result.put(Dimension.TYPE, "mse10");

        enrichmentMessage = collector.getResult().get(0);
        assertEquals(result, enrichmentMessage);

        result.clear();

        locationProcessor.process(messageAssoc, collector);
        locationProcessor.process(messageLocUp, collector);

        result.put(Dimension.CLIENT_ZONE, "ZoneD");
        result.put(Dimension.DOT11STATUS, "ASSOCIATED");
        result.put(Dimension.BYTES, 0);
        result.put(Dimension.WIRELESS_STATION, "AA:AA:AA:AA:AA:AA");
        result.put(Dimension.PKTS, 0);
        result.put(Dimension.TYPE, "mse10");
        result.put(Dimension.CLIENT_CAMPUS, "CampusA");
        result.put(Dimension.CLIENT_BUILDING, "BuildingB");
        result.put(Dimension.TIMESTAMP, 1424767310L);
        result.put(Dimension.CLIENT_MAC, "00:00:00:00:00:00");
        result.put(Dimension.WIRELESS_ID, "rb-corp");
        result.put(Dimension.SENSOR_NAME, "rb-loc");
        result.put(Dimension.CLIENT_FLOOR, "FloorC");
        result.put(Dimension.DOT11PROTOCOL, "IEEE_802_11_B");

        enrichmentMessage = collector.getResult().get(1);
        assertEquals(result, enrichmentMessage);
    }

    @Test
    public void checkName() {
        assertEquals("loc", locationProcessor.getName());
    }
}
