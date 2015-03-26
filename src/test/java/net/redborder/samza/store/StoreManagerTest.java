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
import org.apache.hadoop.util.hash.Hash;
import org.apache.samza.config.Config;

import static org.mockito.Mockito.*;

import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class StoreManagerTest extends TestCase {


    Config config;

    @Mock
    TaskContext context;

    StoreManager storeManager;

    List<String> stores = new ArrayList<>();

    @Before
    public void initTest() throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("src/main/config/enrichment.properties");
        properties.load(inputStream);

        Map<String, String> map = new HashMap<>();

        for (final String name : properties.stringPropertyNames())
            map.put(name, properties.getProperty(name));

        config.putAll(map);

        context = mock(TaskContext.class);

        for (String str : map.keySet()) {
            if (str.contains("stores") && str.contains("factory")) {
                String store = str.substring(str.indexOf("."), str.indexOf(".", str.indexOf(".") + 1));
                stores.add(store);
                when(context.getStore(str)).thenReturn(new MockKeyValueStore());
            }
        }
        storeManager = new StoreManager(config, context);
    }


    @Test
    public void save() {
        for (String store : stores) {
            Map<String, Object> cache = new HashMap<>();
            cache.put("integer", 1);
            cache.put("string", "test");
            cache.put("boolean", true);
            storeManager.getStore(store).put("testing", cache);
        }
    }

    @Test
    public void get() {
        for (String store : stores) {
            Map<String, Object> cache = storeManager.getStore(store).get("testing");
            assertEquals(cache.get("integer"), 1);
            assertEquals(cache.get("string"), "test");
            assertEquals(cache.get("boolean"), true);

        }
    }

    private class MockKeyValueStore implements KeyValueStore<String, Object> {

        Map<Object, Object> store = new HashMap<>();

        @Override
        public Object get(String s) {
            return store.get(s);
        }

        @Override
        public void put(String s, Object o) {
            store.put(s, o);
        }

        @Override
        public void putAll(List<Entry<String, Object>> list) {
        }

        @Override
        public void delete(String s) {
            store.remove(s);
        }

        @Override
        public KeyValueIterator<String, Object> range(String s, String k1) {
            return null;
        }

        @Override
        public KeyValueIterator<String, Object> all() {
            return null;
        }

        @Override
        public void close() {
            store.clear();
            store = null;
        }

        @Override
        public void flush() {
            store.clear();
        }
    }

}

