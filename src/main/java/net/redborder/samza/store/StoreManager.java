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

import net.redborder.samza.util.constants.Dimension;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;

import java.util.*;

public class StoreManager {

    private static Map<String, KeyValueStore<String, Map<String, Object>>> stores = new LinkedHashMap<>();
    private static Map<String, String> storesKeys = new HashMap<>();

    public StoreManager(Config config, TaskContext context) {
        List<String> storesList = config.getList("redborder.stores", Collections.<String>emptyList());

        for (String store : storesList) {
            storesKeys.put(store, config.get("redborder.store." + store + ".key", Dimension.CLIENT_MAC));
            stores.put(store, (KeyValueStore<String, Map<String, Object>>) context.getStore(store));
        }
    }

    public KeyValueStore<String, Map<String, Object>> getStore(String store) {
        return stores.get(store);
    }

    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, Object> enrichment = new HashMap<>();
        enrichment.putAll(message);

        for (Map.Entry<String, KeyValueStore<String, Map<String, Object>>> store : stores.entrySet()) {
            String key = (String) message.get(storesKeys.get(store.getKey()));
            String deployment_id = message.get(Dimension.DEPLOYMENT_ID) == null ? "" : String.valueOf(message.get(Dimension.DEPLOYMENT_ID));
            Map<String, Object> contents = store.getValue().get(key + deployment_id);
            if (contents != null) enrichment.putAll(contents);
        }

        return enrichment;
    }
}
