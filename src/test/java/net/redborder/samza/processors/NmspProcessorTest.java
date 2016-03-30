package net.redborder.samza.processors;

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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;
import static net.redborder.samza.util.constants.DimensionValue.NMSP_TYPE_INFO;
import static net.redborder.samza.util.constants.DimensionValue.NMSP_TYPE_MEASURE;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NmspProcessorTest extends TestCase {
    static MockKeyValueStore storeMeasure;
    static MockKeyValueStore storeInfo;

    static NmspProcessor nmspProcessor;
    static FlowProcessor flowProcessor;

    static EnrichManager enrichManager;

    @Mock
    static StoreManager storeManager;

    @Mock
    static Config config;

    static TaskContext taskContext;


    @BeforeClass
    public static void initTest() {
        // This store uses an in-memory map instead of samza K/V RockDB
        storeMeasure = new MockKeyValueStore();
        storeInfo = new MockKeyValueStore();
        taskContext = new MockTaskContext();
        config = mock(Config.class);

        // Mock the storeManager in order to return the mock store
        // that we just instantiated
        storeManager = mock(StoreManager.class);
        when(config.getInt("redborder.rssiLimit.db", -70)).thenReturn(-70);
        when(storeManager.getStore(NmspProcessor.NMSP_STORE_MEASURE)).thenReturn(storeMeasure);
        when(storeManager.getStore(NmspProcessor.NMSP_STORE_INFO)).thenReturn(storeInfo);
        when(storeManager.enrich(anyMap())).thenAnswer(new Answer<Map<String, Object>>() {
            @Override
            public Map<String, Object> answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                Map<String, Object> measure = storeMeasure.get("00:00:00:00:00:00");
                Map<String, Object> info = storeInfo.get("00:00:00:00:00:00");
                Map<String, Object> result = new HashMap<>((Map<String, Object>) args[0]);

                if (measure != null) {
                    result.putAll(measure);
                }
                if (info != null) {
                    result.putAll(info);
                }

                return result;
            }
        });

        enrichManager = new EnrichManager();
        nmspProcessor = new NmspProcessor(storeManager, enrichManager, config, taskContext);
        flowProcessor = new FlowProcessor(storeManager, enrichManager, config, taskContext);
    }

    @Before
    // Cleans the store in order to use an empty
    // memory map in each test
    public void cleanStore() {
        storeMeasure.flush();
        storeInfo.flush();
    }

    @Test
    public void processEmptyMsg() {
        MockMessageCollector collector = new MockMessageCollector();
        Map<String, Object> message = new HashMap<>();
        nmspProcessor.process(message, collector);
        assertTrue(collector.getResult().isEmpty());
    }

    @Test
    public void emptyMessageIsIgnored() {
        MockMessageCollector collector = new MockMessageCollector();
        Map<String, Object> message = new HashMap<>();
        nmspProcessor.process(message, collector);
        assertTrue(storeMeasure.isEmpty());
    }

    @Test
    public void enrichesWithWirelessStation() {
        MockMessageCollector collector = new MockMessageCollector();

        Map<String, Object> message = new HashMap<>();
        List<String> ap_macs = Arrays.asList("11:11:11:11:11:11", "22:22:22:22:22:22", "33:33:33:33:33:33");
        List<Integer> rssi = Arrays.asList(-80, -54, -32);

        message.put(CLIENT_MAC, "00:00:00:00:00:00");
        message.put(NMSP_AP_MAC, ap_macs);
        message.put(NMSP_RSSI, rssi);
        message.put(TYPE, NMSP_TYPE_MEASURE);
        nmspProcessor.process(message, collector);

        Map<String, Object> fromCache = storeMeasure.get("00:00:00:00:00:00");

        String apFromCache = (String) fromCache.get(WIRELESS_STATION);

        assertEquals(apFromCache, ap_macs.get(2));
    }

    @Test
    public void enrichesWithRssi() {
        MockMessageCollector collector = new MockMessageCollector();

        Map<String, Object> message = new HashMap<>();
        List<String> ap_macs = Arrays.asList("11:11:11:11:11:11", "22:22:22:22:22:22", "33:33:33:33:33:33");
        List<Integer> rssi = Arrays.asList(-80, -54, -32);

        message.put(CLIENT_MAC, "00:00:00:00:00:00");
        message.put(NMSP_AP_MAC, ap_macs);
        message.put(NMSP_RSSI, rssi);
        message.put(TYPE, NMSP_TYPE_MEASURE);
        nmspProcessor.process(message, collector);

        Map<String, Object> fromCache = storeMeasure.get("00:00:00:00:00:00");

        int client_rssi_num = (int) fromCache.get(CLIENT_RSSI_NUM);
        String client_rssi = (String) fromCache.get(CLIENT_RSSI);

        assertEquals("RssiCheck", client_rssi_num, -32);
        assertEquals(client_rssi, "excelent");
    }

    @Test
    public void enrichesWithRssiUsingDeploymendId() {
        MockMessageCollector collector = new MockMessageCollector();

        String namespace = "11111111";

        Map<String, Object> message = new HashMap<>();
        List<String> ap_macs = Arrays.asList("11:11:11:11:11:11", "22:22:22:22:22:22", "33:33:33:33:33:33");
        List<Integer> rssi = Arrays.asList(-80, -54, -32);

        message.put(CLIENT_MAC, "00:00:00:00:00:00");
        message.put(NMSP_AP_MAC, ap_macs);
        message.put(NMSP_RSSI, rssi);
        message.put(NAMESPACE_UUID, namespace);
        message.put(TYPE, NMSP_TYPE_MEASURE);
        nmspProcessor.process(message, collector);

        Map<String, Object> fromCacheA = storeMeasure.get("00:00:00:00:00:00" + namespace);

        int client_rssi_numA = (int) fromCacheA.get(CLIENT_RSSI_NUM);
        String client_rssiA = (String) fromCacheA.get(CLIENT_RSSI);

        assertEquals("RssiCheck", client_rssi_numA, -32);
        assertEquals(client_rssiA, "excelent");

        message.put(CLIENT_MAC, "00:00:00:00:00:00");
        message.put(NMSP_AP_MAC, ap_macs);
        message.put(NMSP_RSSI, rssi);
        message.put(NAMESPACE_UUID, "111111112");
        message.put(TYPE, NMSP_TYPE_MEASURE);
        nmspProcessor.process(message, collector);

        Map<String, Object> fromCacheB = storeMeasure.get("00:00:00:00:00:00");

        assertNull(fromCacheB);
    }

    @Test
    public void enrichWithStatus() {
        MockMessageCollector collector = new MockMessageCollector();

        Map<String, Object> message = new HashMap<>();
        List<String> ap_macs = Arrays.asList("11:11:11:11:11:11", "22:22:22:22:22:22", "33:33:33:33:33:33");
        List<Integer> rssi = Arrays.asList(-80, -54, -32);

        message.put(CLIENT_MAC, "00:00:00:00:00:00");
        message.put(NMSP_AP_MAC, ap_macs);
        message.put(NMSP_RSSI, rssi);
        message.put(TYPE, NMSP_TYPE_MEASURE);

        nmspProcessor.process(message, collector);

        List<Map<String, Object>> toDruid = collector.getResult();

        Map<String, Object> fromCache = storeMeasure.get("00:00:00:00:00:00");

        assertEquals("PROBING", toDruid.get(0).get(DOT11STATUS));
        assertEquals("ASSOCIATED", fromCache.get(DOT11STATUS));
    }

    @Test
    public void enrichWithInfoAndMeasureAPatList() {
        MockMessageCollector collector = new MockMessageCollector();

        //Message 1
        Map<String, Object> messageInfo = new HashMap<>();
        messageInfo.put(CLIENT_MAC, "00:00:00:00:00:00");
        messageInfo.put(TYPE, NMSP_TYPE_INFO);
        messageInfo.put(NMSP_WIRELESS_ID, "rb_Corp");
        messageInfo.put(NMSP_DOT11STATUS, "ASSOCIATED");
        messageInfo.put(WIRELESS_STATION, "33:33:33:33:33:33");

        List<String> ap_macs = Arrays.asList("11:11:11:11:11:11", "22:22:22:22:22:22", "33:33:33:33:33:33");
        List<Integer> rssi = Arrays.asList(-80, -54, -32);
        //Message 2
        Map<String, Object> messageMeasure1 = new HashMap<>();
        messageMeasure1.put(CLIENT_MAC, "00:00:00:00:00:00");
        messageMeasure1.put(NMSP_AP_MAC, ap_macs);
        messageMeasure1.put(NMSP_RSSI, rssi);
        messageMeasure1.put(TYPE, NMSP_TYPE_MEASURE);

        //Message 3
        Map<String, Object> messageMeasure2 = new HashMap<>();
        messageMeasure2.put(CLIENT_MAC, "00:00:00:00:00:00");
        messageMeasure2.put(NMSP_AP_MAC, ap_macs);
        messageMeasure2.put(NMSP_RSSI, rssi);
        messageMeasure2.put(TYPE, NMSP_TYPE_MEASURE);

        Map<String, Object> fromCache;

        nmspProcessor.process(messageMeasure1, collector);

        Map<String, Object> toDruidMeasure1 = collector.getResult().get(0);

        fromCache = storeMeasure.get("00:00:00:00:00:00");

        assertEquals(toDruidMeasure1.get(DOT11STATUS), "PROBING");
        assertEquals(fromCache.get(WIRELESS_ID), null);
        assertEquals(fromCache.get(DOT11STATUS), "ASSOCIATED");

        nmspProcessor.process(messageInfo, collector);

        nmspProcessor.process(messageMeasure2, collector);

        Map<String, Object> toDruidMeasure2 = collector.getResult().get(0);

        fromCache = storeMeasure.get("00:00:00:00:00:00");

        assertEquals(fromCache.get(WIRELESS_ID), "rb_Corp");
        assertEquals(toDruidMeasure2.get(DOT11STATUS), "ASSOCIATED");
        assertEquals(fromCache.get(DOT11STATUS), "ASSOCIATED");
    }

    @Test
    public void enrichWithInfoAndMeasureAPNotatList() {
        MockMessageCollector collector = new MockMessageCollector();

        //Message 1
        Map<String, Object> messageInfo = new HashMap<>();
        messageInfo.put(CLIENT_MAC, "00:00:00:00:00:00");
        messageInfo.put(TYPE, NMSP_TYPE_INFO);
        messageInfo.put(NMSP_WIRELESS_ID, "rb_Corp");
        messageInfo.put(NMSP_DOT11STATUS, "ASSOCIATED");
        messageInfo.put(WIRELESS_STATION, "33:33:33:33:33:33");

        List<String> ap_macs = Arrays.asList("11:11:11:11:11:11", "22:22:22:22:22:22", "33:33:33:33:33:33");
        List<Integer> rssi = Arrays.asList(-80, -54, -32);
        //Message 2
        Map<String, Object> messageMeasure1 = new HashMap<>();
        messageMeasure1.put(CLIENT_MAC, "00:00:00:00:00:00");
        messageMeasure1.put(NMSP_AP_MAC, ap_macs);
        messageMeasure1.put(NMSP_RSSI, rssi);
        messageMeasure1.put(TYPE, NMSP_TYPE_MEASURE);

        //Message 3
        Map<String, Object> messageMeasure2 = new HashMap<>();
        messageMeasure2.put(CLIENT_MAC, "00:00:00:00:00:00");
        messageMeasure2.put(NMSP_AP_MAC, Arrays.asList("11:11:11:11:11:11", "22:22:22:22:22:22"));
        messageMeasure2.put(NMSP_RSSI, Arrays.asList(-80, -54));
        messageMeasure2.put(TYPE, NMSP_TYPE_MEASURE);

        Map<String, Object> fromCache;

        nmspProcessor.process(messageMeasure1, collector);
        messageMeasure1.put(CLIENT_MAC, "00:00:00:00:00:00");

        Map<String, Object> toDruidMeasure1 = collector.getResult().get(0);

        fromCache = storeMeasure.get("00:00:00:00:00:00");

        assertEquals("PROBING", toDruidMeasure1.get(DOT11STATUS));
        assertNull(fromCache.get(WIRELESS_ID));
        assertEquals("ASSOCIATED", fromCache.get(DOT11STATUS));

        nmspProcessor.process(messageInfo, collector);
        messageInfo.put(CLIENT_MAC, "00:00:00:00:00:00");
        collector.getResult();


        nmspProcessor.process(messageMeasure2, collector);
        messageMeasure2.put(CLIENT_MAC, "00:00:00:00:00:00");

        List<Map<String, Object>> toDruidMeasure2 = collector.getResult();

        fromCache = storeMeasure.get("00:00:00:00:00:00");

        assertNull(fromCache.get(WIRELESS_ID));
        assertTrue(toDruidMeasure2.isEmpty());
        assertEquals("ASSOCIATED", fromCache.get(DOT11STATUS));


        messageInfo.put("timestamp", (System.currentTimeMillis() / 1000) - 3600);
        nmspProcessor.process(messageInfo, collector);
        collector.getResult();

        nmspProcessor.process(messageMeasure1, collector);
        Map<String, Object> toDruidMeasureOutOfTime = collector.getResult().get(0);
        System.out.println(toDruidMeasureOutOfTime);

        assertEquals("PROBING", toDruidMeasureOutOfTime.get(DOT11STATUS));
        assertEquals("33:33:33:33:33:33", toDruidMeasureOutOfTime.get(WIRELESS_STATION));
        assertEquals("00:00:00:00:00:00", toDruidMeasureOutOfTime.get(CLIENT_MAC));
    }

    @Test
    public void checkRssiNames() {
        MockMessageCollector collector = new MockMessageCollector();

        //Message 2
        Map<String, Object> messageMeasure1 = new HashMap<>();
        messageMeasure1.put(CLIENT_MAC, "00:00:00:00:00:00");
        messageMeasure1.put(NMSP_AP_MAC, Arrays.asList("11:11:11:11:11:11"));
        messageMeasure1.put(NMSP_RSSI, Arrays.asList(-85));
        messageMeasure1.put(TYPE, NMSP_TYPE_MEASURE);

        nmspProcessor.process(messageMeasure1, collector);
        List<Map<String, Object>> toDruidList = collector.getResult();
        assertEquals(0, toDruidList.size());
        assertEquals(storeMeasure.get("00:00:00:00:00:00").get("client_rssi"), "bad");


        messageMeasure1.put(NMSP_RSSI, Arrays.asList(-80));
        nmspProcessor.process(messageMeasure1, collector);
        assertEquals(0, toDruidList.size());
        assertEquals(storeMeasure.get("00:00:00:00:00:00").get("client_rssi"), "low");

        Map<String, Object> toDruid;
        messageMeasure1.put(NMSP_RSSI, Arrays.asList(-70));
        nmspProcessor.process(messageMeasure1, collector);
        toDruid = collector.getResult().get(0);
        assertEquals(toDruid.get("client_rssi"), "medium");
        assertEquals(storeMeasure.get("00:00:00:00:00:00").get("client_rssi"), "medium");

        messageMeasure1.put(NMSP_RSSI, Arrays.asList(-60));
        nmspProcessor.process(messageMeasure1, collector);
        toDruid = collector.getResult().get(0);
        assertEquals(toDruid.get("client_rssi"), "good");
        assertEquals(storeMeasure.get("00:00:00:00:00:00").get("client_rssi"), "good");

        messageMeasure1.put(NMSP_RSSI, Arrays.asList(-40));
        nmspProcessor.process(messageMeasure1, collector);
        toDruid = collector.getResult().get(0);
        assertEquals(toDruid.get("client_rssi"), "excelent");
        assertEquals(storeMeasure.get("00:00:00:00:00:00").get("client_rssi"), "excelent");

        messageMeasure1.put(NMSP_RSSI, Arrays.asList(0));
        nmspProcessor.process(messageMeasure1, collector);
        toDruid = collector.getResult().get(0);
        assertEquals(toDruid.get("client_rssi"), "unknown");
        assertEquals(storeMeasure.get("00:00:00:00:00:00").get("client_rssi"), "unknown");
    }

    @Test
    public void checkVlanId() {
        MockMessageCollector collector = new MockMessageCollector();

        Map<String, Object> messageInfo = new HashMap<>();
        messageInfo.put(CLIENT_MAC, "00:00:00:00:00:00");
        messageInfo.put(NMSP_AP_MAC, Arrays.asList("11:11:11:11:11:11"));
        messageInfo.put(NMSP_VLAN_ID, 40);

        messageInfo.put(TYPE, NMSP_TYPE_INFO);

        nmspProcessor.process(messageInfo, collector);
        Map<String, Object> toDruid = collector.getResult().get(0);
        assertEquals(toDruid.get(SRC_VLAN), 40);
    }

    @Test
    public void overwriteError() {
        MockMessageCollector nmspCollector = new MockMessageCollector();
        MockMessageCollector flowCollector = new MockMessageCollector();

        //Message 1
        Map<String, Object> messageInfo = new HashMap<>();
        messageInfo.put(CLIENT_MAC, "00:00:00:00:00:00");
        messageInfo.put(TYPE, NMSP_TYPE_INFO);
        messageInfo.put(NMSP_WIRELESS_ID, "rb_Corp");
        messageInfo.put(NMSP_DOT11STATUS, "ASSOCIATED");
        messageInfo.put(WIRELESS_STATION, "33:33:33:33:33:33");
        messageInfo.put(SENSOR_NAME, "WLC");
        messageInfo.put(SENSOR_UUID, "WLC-UUID");

        List<String> ap_macs = Arrays.asList("11:11:11:11:11:11", "22:22:22:22:22:22", "33:33:33:33:33:33");
        List<Integer> rssi = Arrays.asList(-80, -54, -32);
        //Message 2
        Map<String, Object> messageMeasure1 = new HashMap<>();
        messageMeasure1.put(CLIENT_MAC, "00:00:00:00:00:00");
        messageMeasure1.put(NMSP_AP_MAC, ap_macs);
        messageMeasure1.put(NMSP_RSSI, rssi);
        messageMeasure1.put(TYPE, NMSP_TYPE_MEASURE);
        messageMeasure1.put(SENSOR_NAME, "WLC");
        messageMeasure1.put(SENSOR_UUID, "WLC-UUID");

        //Message 3
        Map<String, Object> messageMeasure2 = new HashMap<>();
        messageMeasure2.put(CLIENT_MAC, "00:00:00:00:00:00");
        messageMeasure2.put(NMSP_AP_MAC, ap_macs);
        messageMeasure2.put(NMSP_RSSI, rssi);
        messageMeasure2.put(TYPE, NMSP_TYPE_MEASURE);
        messageMeasure2.put(SENSOR_NAME, "WLC");
        messageMeasure2.put(SENSOR_UUID, "WLC-UUID");

        nmspProcessor.process(messageMeasure1, nmspCollector);
        nmspProcessor.process(messageInfo, nmspCollector);
        nmspProcessor.process(messageMeasure2, nmspCollector);

        Map<String, Object> flowMessage = new HashMap<>();
        flowMessage.put(TIMESTAMP, System.currentTimeMillis() / 1000);
        flowMessage.put(CLIENT_MAC, "00:00:00:00:00:00");
        flowMessage.put(TYPE, "NetflowV10");
        flowMessage.put(SENSOR_NAME, "ASR");
        flowMessage.put(SENSOR_UUID, "ASR-UUID");
        flowMessage.put(DEPLOYMENT, "ASR-DEPLOYMENT");

        flowMessage.put(BYTES, 10L);
        flowMessage.put(PKTS, 4L);

        flowProcessor.process(flowMessage, flowCollector);

        Map<String, Object> result = flowCollector.getResult().get(0);

        assertEquals("ASR", result.get(SENSOR_NAME));
        assertEquals("ASR-UUID", result.get(SENSOR_UUID));

    }

    @Test
    public void checkName() {
        assertEquals("nmsp", nmspProcessor.getName());
    }
}
