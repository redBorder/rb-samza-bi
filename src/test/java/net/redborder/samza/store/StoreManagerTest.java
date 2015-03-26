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

        Map<String, String> map = new HashMap<>();

        for (final String name : properties.stringPropertyNames())
            map.put(name, properties.getProperty(name));

        config = mock(Config.class);
        when(config.keySet()).thenReturn(map.keySet());

        context = mock(TaskContext.class);

        for (String str : map.keySet()) {
            if (str.contains("stores") && str.contains("factory")) {
                String store = str.substring(str.indexOf(".")+1, str.indexOf(".", str.indexOf(".") + 1));
                stores.add(store);
                when(context.getStore(store)).thenReturn(new MockKeyValueStore());
            }
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
    public void enrichment(){
        Map<String, Object> result = new HashMap<>();

        for (String store : stores) {
            Map<String, Object> cache = new HashMap<>();
            cache.put(store + "-mac", store + "-mac");
            result.putAll(cache);
            storeManager.getStore(store).put("testing-mac", cache);
            cache.put(store + "-ip", store + "-ip");
            result.putAll(cache);
            storeManager.getStore(store).put("testing-ip", cache);
        }

        Map<String, Object> enrichCache = storeManager.enrich("testing-mac", "testing-ip");
        assertTrue(enrichCache.equals(result));
    }
}

