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

package net.redborder.samza.processors;

import junit.framework.TestCase;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.MockKeyValueStore;
import net.redborder.samza.util.MockMessageCollector;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;
import static net.redborder.samza.util.constants.DimensionValue.NMSP_TYPE_INFO;
import static net.redborder.samza.util.constants.DimensionValue.NMSP_TYPE_MEASURE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NmspProcessorTest extends TestCase {
    static MockKeyValueStore storeMeasure;
    static MockKeyValueStore storeInfo;

    static NmspProcessor nmspProcessor;

    @Mock
    static StoreManager storeManager;


    @BeforeClass
    public static void initTest() {
        // This store uses an in-memory map instead of samza K/V RockDB
        storeMeasure = new MockKeyValueStore();
        storeInfo = new MockKeyValueStore();

        // Mock the storeManager in order to return the mock store
        // that we just instantiated
        storeManager = mock(StoreManager.class);
        when(storeManager.getStore(NmspProcessor.NMSP_STORE_MEASURE)).thenReturn(storeMeasure);
        when(storeManager.getStore(NmspProcessor.NMSP_STORE_INFO)).thenReturn(storeInfo);

        nmspProcessor = new NmspProcessor(storeManager);
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

        assertEquals(toDruid.get(0).get(DOT11STATUS), "PROBING");
        assertEquals(fromCache.get(DOT11STATUS), "ASSOCIATED");
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

        Map<String, Object> toDruidMeasure1 = collector.getResult().get(0);

        fromCache = storeMeasure.get("00:00:00:00:00:00");

        assertEquals("PROBING", toDruidMeasure1.get(DOT11STATUS));
        assertNull(fromCache.get(WIRELESS_ID));
        assertEquals("ASSOCIATED", fromCache.get(DOT11STATUS));

        nmspProcessor.process(messageInfo, collector);
        collector.getResult();

        nmspProcessor.process(messageMeasure2, collector);

        Map<String, Object> toDruidMeasure2 = collector.getResult().get(0);

        fromCache = storeMeasure.get("00:00:00:00:00:00");

        assertNull(fromCache.get(WIRELESS_ID));
        assertEquals("PROBING", toDruidMeasure2.get(DOT11STATUS));
        assertEquals("ASSOCIATED", fromCache.get(DOT11STATUS));
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
        Map<String, Object> toDruid = collector.getResult().get(0);
        assertEquals(toDruid.get("client_rssi"), "bad");

        messageMeasure1.put(NMSP_RSSI, Arrays.asList(-80));
        nmspProcessor.process(messageMeasure1, collector);
        toDruid = collector.getResult().get(0);
        assertEquals(toDruid.get("client_rssi"), "low");

        messageMeasure1.put(NMSP_RSSI, Arrays.asList(-70));
        nmspProcessor.process(messageMeasure1, collector);
        toDruid = collector.getResult().get(0);
        assertEquals(toDruid.get("client_rssi"), "medium");

        messageMeasure1.put(NMSP_RSSI, Arrays.asList(-60));
        nmspProcessor.process(messageMeasure1, collector);
        toDruid = collector.getResult().get(0);
        assertEquals(toDruid.get("client_rssi"), "good");

        messageMeasure1.put(NMSP_RSSI, Arrays.asList(-40));
        nmspProcessor.process(messageMeasure1, collector);
        toDruid = collector.getResult().get(0);
        assertEquals(toDruid.get("client_rssi"), "excelent");

        messageMeasure1.put(NMSP_RSSI, Arrays.asList(0));
        nmspProcessor.process(messageMeasure1, collector);
        toDruid = collector.getResult().get(0);
        assertEquals(toDruid.get("client_rssi"), "unknown");
    }

    @Test
    public void checkVlanId(){
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
}
