package net.redborder.samza.processors;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;
import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.MockKeyValueStore;
import net.redborder.samza.util.MockMessageCollector;
import net.redborder.samza.util.MockTaskContext;
import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RadiusProcessorTest extends TestCase {
    static MockKeyValueStore storeRadius;
    ObjectMapper objectMapper = new ObjectMapper();
    String radiusEvent = "{\n" +
            "  \"timestamp\": 1441277328,\n" +
            "  \"packet_type\": \"Accounting-Request\",\n" +
            "  \"Packet-Src-IP-Address\": \"128.136.224.78\",\n" +
            "  \"Packet-Dst-IP-Address\": \"0.0.0.0\",\n" +
            "  \"Packet-Src-IP-Port\": \"57212\",\n" +
            "  \"Packet-Dst-IP-Port\": \"1813\",\n" +
            "  \"User-Name\": \"iojeda@cisco.com\",\n" +
            "  \"Operator-Name\": \"1spg.com\",\n" +
            "  \"NAS-Port\": \"1\",\n" +
            "  \"NAS-IP-Address\": \"128.136.224.78\",\n" +
            "  \"Framed-IP-Address\": \"192.168.2.119\",\n" +
            "  \"NAS-Identifier\": \"SPG\",\n" +
            "  \"Airespace-Wlan-Id\": \"19\",\n" +
            "  \"Acct-Session-Id\": \"55e76e1a\\/e0:b5:2d:16:e9:10\\/142\",\n" +
            "  \"NAS-Port-Type\": \"Wireless-802.11\",\n" +
            "  \"Cisco-AVPair\": \"audit-session-id=4ee08880000000701a6ee755\",\n" +
            "  \"Acct-Authentic\": \"RADIUS\",\n" +
            "  \"Tunnel-Type\": \"VLAN\",\n" +
            "  \"Tunnel-Medium-Type\": \"IEEE-802\",\n" +
            "  \"Tunnel-Private-Group-Id\": \"224\",\n" +
            "  \"Event-Timestamp\": \"Sep  2 2015 21:54:25 UTC\",\n" +
            "  \"Acct-Status-Type\": \"Start\",\n" +
            "  \"Acct-Input-Octets\": \"116335\",\n" +
            "  \"Acct-Input-Gigawords\": \"0\",\n" +
            "  \"Acct-Output-Octets\": \"215106\",\n" +
            "  \"Acct-Output-Gigawords\": \"0\",\n" +
            "  \"Acct-Input-Packets\": \"806\",\n" +
            "  \"Acct-Output-Packets\": \"447\",\n" +
            "  \"Acct-Terminate-Cause\": \"User-Request\",\n" +
            "  \"Acct-Session-Time\": \"500\",\n" +
            "  \"Acct-Delay-Time\": \"0\",\n" +
            "  \"Calling-Station-Id\": \"e0:b5:2d:16:e9:10\",\n" +
            "  \"Called-Station-Id\": \"F0:29:29:92:47:C0:SPG-HS20\",\n" +
            "  \"Acct-Unique-Session-Id\": \"7254e42d3edda324\", \n" +
            "  \"enrichment\":{\"sensor_uuid\":23131}}";
    static RadiusProcessor radiusProcessor;
    static EnrichManager enrichManager;

    @Mock
    static StoreManager storeManager;

    @Mock
    static Config config;

    static TaskContext taskContext;


    @BeforeClass
    public static void initTest() {
        // This store uses an in-memory map instead of samza K/V RockDB
        storeRadius = new MockKeyValueStore();
        taskContext = new MockTaskContext();
        config = mock(Config.class);

        // Mock the storeManager in order to return the mock store
        // that we just instantiated
        storeManager = mock(StoreManager.class);
        when(storeManager.getStore(RadiusProcessor.RADIUS_STORE)).thenReturn(storeRadius);
        when(storeManager.enrich(anyMap())).thenAnswer(new Answer<Map<String, Object>>() {
            @Override
            public Map<String, Object> answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                return (Map<String, Object>) args[0];
            }
        });

        enrichManager = new EnrichManager();
        radiusProcessor = new RadiusProcessor(storeManager, enrichManager, config, taskContext);
    }

    @Before
    // Cleans the store in order to use an empty
    // memory map in each test
    public void cleanStore() {
        storeRadius.flush();
    }

    @Test
    public void processEmptyMsg() {
        MockMessageCollector collector = new MockMessageCollector();
        Map<String, Object> message = new HashMap<>();
        radiusProcessor.process(message, collector);
        assertTrue(collector.getResult().isEmpty());
    }

    @Test
    public void emptyMessageIsIgnored() {
        MockMessageCollector collector = new MockMessageCollector();
        Map<String, Object> message = new HashMap<>();
        radiusProcessor.process(message, collector);
        assertTrue(storeRadius.isEmpty());
    }

    @Test
    public void checkRadiusStore() throws IOException {
        MockMessageCollector collector = new MockMessageCollector();

        Map<String, Object> message = objectMapper.readValue(radiusEvent, Map.class);
        message.remove(ACCT_STATUS_TYPE);
        radiusProcessor.process(message, collector);
        Map<String, Object> fromCache = storeRadius.get("e0:b5:2d:16:e9:10");
        assertNotNull(fromCache);
    }

    @Test
    public void checkRadiusStoreAdd() throws IOException {
        MockMessageCollector collector = new MockMessageCollector();

        Map<String, Object> message = objectMapper.readValue(radiusEvent, Map.class);
        message.put(ACCT_STATUS_TYPE, "Start");
        radiusProcessor.process(message, collector);
        Map<String, Object> fromCache = storeRadius.get("e0:b5:2d:16:e9:10");
        assertNotNull(fromCache);
        fromCache = storeRadius.get("e0:b5:2d:16:e9:XX");
        assertNull(fromCache);
    }

   /* @Test
    public void checkRadiusStoreAddandRemove() throws IOException {
        MockMessageCollector collector = new MockMessageCollector();

        Map<String, Object> message = objectMapper.readValue(radiusEvent, Map.class);
        message.put(ACCT_STATUS_TYPE, "Start");
        radiusProcessor.process(message, collector);
        Map<String, Object> fromCache = storeRadius.get("e0:b5:2d:16:e9:10");
        assertNotNull(fromCache);
        message.put(ACCT_STATUS_TYPE, "Stop");
        radiusProcessor.process(message, collector);
        fromCache = storeRadius.get("e0:b5:2d:16:e9:10");
        assertNull(fromCache);
    }*/

    @Test
    public void checkWirelessStationAndWirelessId() throws IOException {
        MockMessageCollector collector = new MockMessageCollector();

        Map<String, Object> message = objectMapper.readValue(radiusEvent, Map.class);
        message.put(ACCT_STATUS_TYPE, "Start");
        radiusProcessor.process(message, collector);
        Map<String, Object> fromCache = storeRadius.get("e0:b5:2d:16:e9:10");
        assertEquals("f0:29:29:92:47:c0", fromCache.get(WIRELESS_STATION));
        assertEquals("SPG-HS20", fromCache.get(WIRELESS_ID));
    }

    @Test
    public void getNameTest(){
        assertEquals("radius", radiusProcessor.getName());
    }

    @Test
    public void checkNullTimestamp() throws IOException {
        MockMessageCollector collector = new MockMessageCollector();

        Map<String, Object> message = objectMapper.readValue(radiusEvent, Map.class);
        message.put(ACCT_STATUS_TYPE, "Start");
        message.remove(TIMESTAMP);
        radiusProcessor.process(message, collector);
        assertTrue(collector.getResult().get(0).containsKey(TIMESTAMP));
    }

    @Test
    public void checkClientConnection() throws IOException {
        MockMessageCollector collector = new MockMessageCollector();

        Map<String, Object> message = objectMapper.readValue(radiusEvent, Map.class);
        message.put(ACCT_STATUS_TYPE, "Start");
        radiusProcessor.process(message, collector);
        assertEquals("start", collector.getResult().get(0).get(CLIENT_ACCOUNTING_TYPE));
    }

    @Test
    public void checkEnrichment() throws IOException {
        MockMessageCollector collector = new MockMessageCollector();

        Map<String, Object> message = objectMapper.readValue(radiusEvent, Map.class);
        message.put(ACCT_STATUS_TYPE, "Start");
        radiusProcessor.process(message, collector);
        assertEquals(23131, collector.getResult().get(0).get(SENSOR_UUID));
    }}
