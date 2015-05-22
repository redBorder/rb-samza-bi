package net.redborder.samza.processors;

import junit.framework.TestCase;
import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.MockKeyValueStore;
import net.redborder.samza.util.MockMessageCollector;
import net.redborder.samza.util.MockTaskContext;
import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;

import static net.redborder.samza.util.constants.Dimension.*;
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
        taskContext = new MockTaskContext();
        config = mock(Config.class);

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
        String namespace_id = "11111111-1111-1111-1111-111111111111";

        Map<String, Object> content = new HashMap<>();
        content.put(TIMESTAMP, "2015-03-31T02:57:38.570-0700");
        content.put(LOC_SUBSCRIPTION_NAME, "sensor-testing");
        content.put(LOC_ENTITY, "WIRELESS_CLIENTS");
        content.put(LOC_DEVICEID, "00:00:00:00:00:00");
        content.put(LOC_MSEUDI, "AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8");
        content.put(LOC_TIMESTAMP_MILLIS, Long.valueOf(1427795858570L));

        Map<String, Object> location = new HashMap<>();
        location.put(LOC_MACADDR, "00:00:00:00:00:00");
        location.put(LOC_SSID, "rb-corp");
        location.put(LOC_BAND, "UNKNOWN");
        location.put(LOC_AP_MACADDR, "68:bc:0c:65:0a:a0");
        location.put(LOC_DOT11STATUS, "ASSOCIATED");
        location.put(LOC_IPADDR, Arrays.asList(new String[]{"10.50.22.1", "fe80:0000:0000:0000:102c:0f0e:db63:7e40"}));

        Map<String, Object> mapInfo = new HashMap<>();
        mapInfo.put(LOC_MAP_HIERARCHY, "Campus Test>Building Test>Floor Test");

        Map<String, Object> mapCoordinate = new HashMap<>();
        mapCoordinate.put(LOC_COORDINATE_X, 88.609215);
        mapCoordinate.put(LOC_COORDINATE_Y, 72.91531);
        mapCoordinate.put(LOC_COORDINATE_UNIT, "FEET");

        location.put(LOC_MAPCOORDINATEv8, mapCoordinate);
        location.put(LOC_MAPINFOv8, mapInfo);
        content.put(LOC_LOCATION, location);
        message.put(LOC_STREAMING_NOTIFICATION, content);
        message.put(NAMESPACE_ID, namespace_id);

        locationProcessor.process(message, collector);

        result.put(DOT11STATUS, "ASSOCIATED");
        result.put(BYTES, 0);
        result.put(PKTS, 0);
        result.put(TYPE, "mse");
        result.put(CAMPUS, "Campus Test");
        result.put(BUILDING, "Building Test");
        result.put(TIMESTAMP, Long.valueOf(1427795858L));
        result.put(CLIENT_MAC, "00:00:00:00:00:00");
        result.put(WIRELESS_ID, "rb-corp");
        result.put(SENSOR_NAME, "sensor-testing");
        result.put(FLOOR, "Floor Test");
        result.put(CLIENT_RSSI, "unknown");
        result.put(CLIENT_RSSI_NUM, 0);
        result.put(CLIENT_SNR, "unknown");
        result.put(CLIENT_SNR_NUM, 0);
        result.put(SRC, "10.50.22.1");
        result.put(WIRELESS_STATION, "68:bc:0c:65:0a:a0");
        result.put(NAMESPACE_ID, namespace_id);

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
        String namespace_id = "11111111-1111-1111-1111-111111111111";

        Map<String, Object> messageLocUp = new HashMap<>();
        contentLoc.put(LOC_NOTIFICATION_TYPE, "locationupdate");
        contentLoc.put(LOC_SUBSCRIPTION_NAME, "rb-loc");
        contentLoc.put(LOC_DEVICEID, "00:00:00:00:00:00");
        contentLoc.put(LOC_ENTITY, "WIRELESS_CLIENTS");
        contentLoc.put(LOC_SSID, "rb-corp");
        contentLoc.put(LOC_BAND, null);
        contentLoc.put(LOC_AP_MACADDR, "AA:AA:AA:AA:AA:AA");
        contentLoc.put(LOC_MAP_HIERARCHY_V10, "CampusA>BuildingB>FloorC>ZoneD");
        contentLoc.put(TIMESTAMP, Long.valueOf(1424767310026L));
        Map<String, Object> coordinate = new HashMap<>();
        coordinate.put(LOC_COORDINATE_X, 171.231);
        coordinate.put(LOC_COORDINATE_Y, 1.241);
        coordinate.put(LOC_COORDINATE_Z, 0.0);
        contentLoc.put(LOC_COORDINATE, coordinate);
        contentLoc.put(NAMESPACE_ID, namespace_id);
        contentsLoc.add(contentLoc);
        messageLocUp.put(LOC_NOTIFICATIONS, contentsLoc);

        locationProcessor.process(messageLocUp, collector);

        result.put(TIMESTAMP, Long.valueOf(1424767310L));
        result.put(ZONE, "ZoneD");
        result.put(FLOOR, "FloorC");
        result.put(BUILDING, "BuildingB");
        result.put(CAMPUS, "CampusA");
        result.put(FLOOR, "FloorC");
        result.put(CLIENT_MAC, "00:00:00:00:00:00");
        result.put(DOT11STATUS, "PROBING");
        result.put(WIRELESS_STATION, "AA:AA:AA:AA:AA:AA");
        result.put(PKTS, 0);
        result.put(BYTES, 0);
        result.put(TYPE, "mse10");
        result.put(SENSOR_NAME, "rb-loc");
        result.put(NAMESPACE_ID, namespace_id);

        enrichmentMessage = collector.getResult().get(0);
        assertEquals(result, enrichmentMessage);

        result.clear();

        List<Map<String, Object>> contentsAssoc = new ArrayList<>();
        Map<String, Object> contentAssoc = new HashMap<>();

        Map<String, Object> messageAssoc = new HashMap<>();
        contentAssoc.put(LOC_NOTIFICATION_TYPE, "association");
        contentAssoc.put(LOC_SUBSCRIPTION_NAME, "rb-assoc");
        contentAssoc.put(LOC_DEVICEID, "00:00:00:00:00:00");
        contentAssoc.put(LOC_ENTITY, "WIRELESS_CLIENTS");
        contentAssoc.put(LOC_SSID, "rb-corp");
        contentAssoc.put(LOC_BAND, "IEEE_802_11_B");
        contentAssoc.put(LOC_AP_MACADDR, "AA:AA:AA:AA:AA:AA");
        contentAssoc.put(LOC_IPADDR, Arrays.asList(new String[]{"25.145.34.131"}));
        contentAssoc.put(LOC_STATUS, 3);
        contentAssoc.put(LOC_USERNAME, "");
        contentAssoc.put(TIMESTAMP, Long.valueOf(1424767310026L));
        contentAssoc.put(NAMESPACE_ID, namespace_id);
        contentsAssoc.add(contentAssoc);
        messageAssoc.put(LOC_NOTIFICATIONS, contentsAssoc);

        locationProcessor.process(messageAssoc, collector);

        result.put(TIMESTAMP, Long.valueOf(1424767310L));
        result.put(CLIENT_MAC, "00:00:00:00:00:00");
        result.put(WIRELESS_ID, "rb-corp");
        result.put(DOT11STATUS, "ASSOCIATED");
        result.put(PKTS, 0);
        result.put(BYTES, 0);
        result.put(SENSOR_NAME, "rb-assoc");
        result.put(WIRELESS_STATION, "AA:AA:AA:AA:AA:AA");
        result.put(DOT11PROTOCOL, "IEEE_802_11_B");
        result.put(TYPE, "mse10");

        enrichmentMessage = collector.getResult().get(0);
        assertEquals(result, enrichmentMessage);

        result.clear();

        locationProcessor.process(messageAssoc, collector);
        locationProcessor.process(messageLocUp, collector);

        result.put(ZONE, "ZoneD");
        result.put(DOT11STATUS, "ASSOCIATED");
        result.put(BYTES, 0);
        result.put(WIRELESS_STATION, "AA:AA:AA:AA:AA:AA");
        result.put(PKTS, 0);
        result.put(TYPE, "mse10");
        result.put(CAMPUS, "CampusA");
        result.put(BUILDING, "BuildingB");
        result.put(TIMESTAMP, Long.valueOf(1424767310L));
        result.put(CLIENT_MAC, "00:00:00:00:00:00");
        result.put(WIRELESS_ID, "rb-corp");
        result.put(SENSOR_NAME, "rb-loc");
        result.put(FLOOR, "FloorC");
        result.put(DOT11PROTOCOL, "IEEE_802_11_B");
        result.put(NAMESPACE_ID, namespace_id);

        enrichmentMessage = collector.getResult().get(1);
        assertEquals(result, enrichmentMessage);

        result.clear();

        contentLoc.put(NAMESPACE_ID, "11111111-1111-1111-1111-111111111112");
        contentsLoc.clear();
        contentsLoc.add(contentLoc);
        messageLocUp.put(LOC_NOTIFICATIONS, contentsLoc);

        locationProcessor.process(messageAssoc, collector);
        locationProcessor.process(messageLocUp, collector);

        result.put(ZONE, "ZoneD");
        result.put(DOT11STATUS, "PROBING");
        result.put(BYTES, 0);
        result.put(WIRELESS_STATION, "AA:AA:AA:AA:AA:AA");
        result.put(PKTS, 0);
        result.put(TYPE, "mse10");
        result.put(CAMPUS, "CampusA");
        result.put(BUILDING, "BuildingB");
        result.put(TIMESTAMP, Long.valueOf(1424767310L));
        result.put(CLIENT_MAC, "00:00:00:00:00:00");
        result.put(SENSOR_NAME, "rb-loc");
        result.put(FLOOR, "FloorC");
        result.put(NAMESPACE_ID, "11111111-1111-1111-1111-111111111112");

        enrichmentMessage = collector.getResult().get(1);
        assertEquals(result, enrichmentMessage);
    }

    @Test
    public void checkName() {
        assertEquals("loc", locationProcessor.getName());
    }
}
