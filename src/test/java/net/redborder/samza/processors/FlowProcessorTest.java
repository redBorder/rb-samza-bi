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
import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.MockKeyValueStore;
import net.redborder.samza.util.MockMessageCollector;
import net.redborder.samza.util.constants.Dimension;
import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FlowProcessorTest extends TestCase {
    static FlowProcessor flowProcessor;
    static StoreManager storeManager;
    static EnrichManager enrichManager;

    @Mock
    static Config config;

    @Mock
    static TaskContext context;

    static List<String> stores = new ArrayList<>();

    @BeforeClass
    public static void initTest() throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = new FileInputStream("src/main/config/enrichment.properties");
        properties.load(inputStream);

        context = mock(TaskContext.class);

        String storesListAsString = properties.getProperty("redborder.stores");
        for (String store : storesListAsString.split(",")) {
            stores.add(store);
            when(context.getStore(store)).thenReturn(new MockKeyValueStore());
        }

        config = mock(Config.class);
        when(config.getList("redborder.stores")).thenReturn(stores);

        storeManager = new StoreManager(config, context);
        enrichManager = new EnrichManager();
        flowProcessor = new FlowProcessor(storeManager, enrichManager);
    }

    @Test
    public void enrichment() {
        Map<String, Object> cacheNmsp = new HashMap<>();
        MockMessageCollector collector = new MockMessageCollector();

        if (stores.contains("nmsp-measure")) {
            cacheNmsp.put("wireless_station", "11:11:11:11:11:11");
            cacheNmsp.put("dot11Status", "ASSOCIATED");
            storeManager.getStore("nmsp-measure").put("00:00:00:00:00:00", cacheNmsp);
        }

        Map<String, Object> message = new HashMap<>();

        message.put(Dimension.CLIENT_MAC, "00:00:00:00:00:00");
        flowProcessor.process(message, collector);

        Map<String, Object> result = collector.getResult().get(0);

        message.putAll(cacheNmsp);
        assertTrue(message.equals(result));
    }
}

