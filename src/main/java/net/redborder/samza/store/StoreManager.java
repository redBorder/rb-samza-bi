package net.redborder.samza.store;

import net.redborder.samza.processors.LocationLogicProcessor;
import net.redborder.samza.util.PostgresqlManager;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static net.redborder.samza.util.constants.Dimension.*;

public class StoreManager {

    private static Map<String, Store> stores = new LinkedHashMap<>();
    private static final Logger log = LoggerFactory.getLogger(StoreManager.class);
    private List<String> storesList;

    public StoreManager(Config config, TaskContext context) {
        storesList = config.getList("redborder.stores", Collections.<String>emptyList());

        log.info("Making stores: ");
        for (String store : storesList) {
            if (!stores.containsKey(store)) {
                Store storeData = new Store();
                storeData.setKeys(config.getList("redborder.store." + store + ".keys", Arrays.asList(CLIENT_MAC, NAMESPACE_UUID)));
                storeData.setOverwrite(config.getBoolean("redborder.store." + store + ".overwrite", true));
                storeData.setStore((KeyValueStore<String, Map<String, Object>>) context.getStore(store));
                log.info("  * Store: {} {}", store, storeData.toString());
                stores.put(store, storeData);
            }
        }
    }

    public KeyValueStore<String, Map<String, Object>> getStore(String store) {
        Store storeData = stores.get(store);
        KeyValueStore<String, Map<String, Object>> keyValueStore = null;

        if (storeData != null) {
            keyValueStore = storeData.getStore();
        }

        return keyValueStore;
    }

    public boolean hasOverwriteEnabled(String store) {
        Store storeData = stores.get(store);
        boolean overwrite = true;

        if (storeData != null) {
            overwrite = storeData.mustOverwrite();
        }

        return overwrite;
    }

    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, Object> enrichment = new HashMap<>(message);

        for (String store : storesList) {
            if (store.equals(PostgresqlManager.SENSOR_PSQL_STORE) || store.equals(PostgresqlManager.WLC_PSQL_STORE)) {
                Store storeData = stores.get(store);
                List<String> keys = storeData.getKeys();
                Object namespace = message.get(NAMESPACE_UUID);

                StringBuilder builder = new StringBuilder();

                for (String key : keys) {
                    Object kv = enrichment.get(key);
                    if (kv != null) {
                        builder.append(kv);
                    }
                }

                String mergeKey = builder.toString();
                KeyValueStore<String, Map<String, Object>> keyValueStore = storeData.getStore();
                Map<String, Object> contents = keyValueStore.get(mergeKey);

                if (contents == null) {
                    Object key = enrichment.get(keys.get(0));

                    if (key != null) {
                        contents = keyValueStore.get(key.toString());
                    }
                }

                if (contents != null) {
                    String psqlNamespace = (String) contents.get(NAMESPACE_UUID);
                    if (namespace != null && psqlNamespace != null) {
                        if(namespace.equals(psqlNamespace)){
                            if (storeData.mustOverwrite()) {
                                enrichment.putAll(contents);
                            } else {
                                Map<String, Object> newData = new HashMap<>();
                                newData.putAll(contents);
                                newData.putAll(enrichment);
                                enrichment = newData;
                            }
                        }
                    } else {
                        if (storeData.mustOverwrite()) {
                            enrichment.putAll(contents);
                        } else {
                            Map<String, Object> newData = new HashMap<>();
                            newData.putAll(contents);
                            newData.putAll(enrichment);
                            enrichment = newData;
                        }
                    }
                }
            } else if (!store.equals(LocationLogicProcessor.LOCATION_STORE_LOGIC)) {
                Store storeData = stores.get(store);
                List<String> keys = storeData.getKeys();
                StringBuilder builder = new StringBuilder();

                for (String key : keys) {
                    Object kv = enrichment.get(key);
                    if (kv != null) {
                        builder.append(kv);
                    }
                }


                String mergeKey = builder.toString();
                KeyValueStore<String, Map<String, Object>> keyValueStore = storeData.getStore();
                Map<String, Object> contents = keyValueStore.get(mergeKey);

                if (contents == null) {
                    Object key = enrichment.get(keys.get(0));

                    if (key != null) {
                        contents = keyValueStore.get(key.toString());
                    }
                }

                log.debug("msgType: " + message.get(TYPE) + " store: " + store + " key: " + enrichment.get(keys.get(0)) + " mergeKey: {} - contents: {}", mergeKey, contents);

                if (contents != null) {
                    if (storeData.mustOverwrite()) {
                        enrichment.putAll(contents);
                    } else {
                        Map<String, Object> newData = new HashMap<>();
                        newData.putAll(contents);
                        newData.putAll(enrichment);
                        enrichment = newData;
                    }
                }
            }
        }

        return enrichment;
    }

    private class Store {
        private List<String> keys;
        private boolean overwrite;
        private KeyValueStore<String, Map<String, Object>> store;

        public void setStore(KeyValueStore<String, Map<String, Object>> store) {
            this.store = store;
        }

        public void setKeys(List<String> keys) {
            this.keys = keys;
        }

        public void setOverwrite(boolean overwrite) {
            this.overwrite = overwrite;
        }

        public KeyValueStore<String, Map<String, Object>> getStore() {
            return store;
        }

        public List<String> getKeys() {
            return keys;
        }

        public boolean mustOverwrite() {
            return overwrite;
        }

        @Override
        public String toString() {
            return new StringBuffer()
                    .append("KEYS: ").append(keys).append(" ")
                    .append("OVERWRITE: ").append(overwrite).toString();
        }
    }
}
