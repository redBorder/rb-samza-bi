package net.redborder.samza.store;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 26/3/15 10:32.
 */
public class StoreManager {

    private static Map<String, KeyValueStore<String, Map<String, Object>>> stores = new HashMap<>();

    public static void init(Config config, TaskContext context){
        for(String str : config.keySet()){
            if(str.contains("stores") && str.contains("factory")){
                String store = str.substring(str.indexOf("."), str.indexOf(".", str.indexOf(".")+1));
                stores.put(store, (KeyValueStore<String, Map<String, Object>>) context.getStore(store));
            }
        }
        stores.putAll(stores);
    }

    public static KeyValueStore<String, Map<String, Object>> getStore(String store){
        return stores.get(store);
    }

    public static Map<String, Object> enrich(String key){
        Map<String, Object> enrichment = new HashMap<>();
        for(KeyValueStore<String, Map<String, Object>> store : stores.values())
            enrichment.putAll(store.get(key));
        return enrichment;
    }
}
