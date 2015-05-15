package net.redborder.samza.store;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;

import java.util.*;

import static net.redborder.samza.util.constants.Dimension.*;

public class StoreManager {

    private static Map<String, KeyValueStore<String, Map<String, Object>>> stores = new LinkedHashMap<>();
    private static Map<String, String> storesKeys = new HashMap<>();

    public StoreManager(Config config, TaskContext context) {
        List<String> storesList = config.getList("redborder.stores", Collections.<String>emptyList());

        for (String store : storesList) {
            storesKeys.put(store, config.get("redborder.store." + store + ".key", CLIENT_MAC));
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
            String namespace_id = message.get(NAMESPACE_ID) == null ? "" : String.valueOf(message.get(NAMESPACE_ID));
            Map<String, Object> contents = store.getValue().get(key + namespace_id);
            if (contents != null) enrichment.putAll(contents);
        }

        return enrichment;
    }
}
