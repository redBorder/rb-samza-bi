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

package net.redborder.samza.store;

import junit.framework.TestCase;
import net.redborder.samza.util.MockKeyValueStore;
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

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class StoreManagerTest extends TestCase {

    @Mock
    static Config config;

    @Mock
    static TaskContext context;

    static StoreManager storeManager;

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
        for (String store : stores) {
            when(config.get("redborder.store." + store + ".key", Dimension.CLIENT_MAC)).thenReturn(properties.getProperty("redborder.store." + store + ".key"));
        }

        storeManager = new StoreManager(config, context);
    }

    @Test
    public void checkStore() {
        for (String store : stores) {
            Map<String, Object> cache = new HashMap<>();
            cache.put("integer", 1);
            cache.put("string", "test");
            cache.put("boolean", true);
            storeManager.getStore(store).put("testing", cache);
        }

        for (String store : stores) {
            Map<String, Object> cache = storeManager.getStore(store).get("testing");
            assertEquals(cache.get("integer"), 1);
            assertEquals(cache.get("string"), "test");
            assertEquals(cache.get("boolean"), true);
            assertEquals(cache.get("null"), null);
        }
    }

    @Test
    public void enrichment() {
        Map<String, Object> result = new HashMap<>();

        Map<String, Object> message = new HashMap<>();
        message.put(Dimension.CLIENT_MAC, "testing-mac");

        result.putAll(message);

        for (String store : stores) {
            Map<String, Object> cache = new HashMap<>();
            cache.put(store + "-enrichment-key", store + "-enrichment-value");
            result.putAll(cache);
            storeManager.getStore(store).put("testing-mac", cache);
        }

        Map<String, Object> enrichCache = storeManager.enrich(message);
        assertTrue(enrichCache.equals(result));
    }
}

