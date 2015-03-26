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
import net.redborder.samza.util.constants.Dimension;
import net.redborder.samza.util.constants.DimensionValue;
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
    }

    @Test
    public void processReturnsNull() {
        Map<String, Object> message = new HashMap<>();
        assertEquals(nmspProcessor.process(message), null);
    }

    @Test
    public void emptyMessageIsIgnored() {
        Map<String, Object> message = new HashMap<>();
        nmspProcessor.process(message);
        assertTrue(storeMeasure.isEmpty());
    }

    @Test
    public void enrichesWithWirelessStation() {
        Map<String, Object> message = new HashMap<>();
        List<String> ap_macs = Arrays.asList("11:11:11:11:11:11", "22:22:22:22:22:22", "33:33:33:33:33:33");

        message.put(Dimension.CLIENT_MAC, "00:00:00:00:00:00");
        message.put(Dimension.NMSP_AP_MAC, ap_macs);
        message.put(Dimension.TYPE, DimensionValue.NMSP_TYPE_MEASURE);
        nmspProcessor.process(message);

        Map<String, Object> fromCache = storeMeasure.get("00:00:00:00:00:00");
        String apFromCache = (String) fromCache.get(Dimension.WIRELESS_STATION);

        assertEquals(apFromCache, ap_macs.get(0));
    }
}
