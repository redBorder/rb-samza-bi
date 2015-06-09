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

import static net.redborder.samza.util.constants.Dimension.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        when(config.getList("redborder.stores", Collections.<String>emptyList())).thenReturn(stores);
        for (String store : stores) {
            String storeKey = properties.getProperty("redborder.store." + store + ".key");
            String storeOverwriteStr = properties.getProperty("redborder.store." + store + ".overwrite");
            boolean storeOverwrite = (storeOverwriteStr == null || storeOverwriteStr == "true");

            when(config.get("redborder.store." + store + ".key", CLIENT_MAC)).thenReturn(storeKey);
            when(config.getBoolean("redborder.store." + store + ".overwrite", true)).thenReturn(storeOverwrite);
        }

        storeManager = new StoreManager(config, context, Dimension.NAMESPACE_UUID, Dimension.CLIENT_MAC);
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
    public void enrichmentUsingNamespaceId() {
        Map<String, Object> result = new HashMap<>();

        String namespace_id_a = "tenant_A";
        String namespace_id_b = "tenant_B";

        Map<String, Object> message = new HashMap<>();
        message.put(CLIENT_MAC, "testing-mac");
        message.put(WIRELESS_STATION, "testing-mac");
        message.put(NAMESPACE_UUID, namespace_id_a);

        result.putAll(message);

        for (String store : stores) {
            Map<String, Object> cache = new HashMap<>();
            cache.put(store + "-enrichment-key" + namespace_id_a, store + "-enrichment-value");
            result.putAll(cache);
            storeManager.getStore(store).put("testing-mac" + namespace_id_a, cache);
        }

        Map<String, Object> enrichCache = storeManager.enrich(message);
        assertEquals(result, enrichCache);

        message.put(NAMESPACE_UUID, namespace_id_b);

        Map<String, Object> enrichCacheWithoutNamespace = storeManager.enrich(message);
        assertEquals(message, enrichCacheWithoutNamespace);
    }

    @Test
    public void enrichmentWithoutOverwrite() {
        Map<String, Object> result = new HashMap<>();

        Map<String, Object> message = new HashMap<>();
        message.put(CLIENT_MAC, "testing-mac");
        message.put(WIRELESS_STATION, "testing-mac");
        message.put(BUILDING, "testing-building");

        result.putAll(message);

        Map<String, Object> cache = new HashMap<>();
        cache.put(BUILDING, "postgresql-testing-building");
        storeManager.getStore("postgresql").put("testing-mac", cache);

        Map<String, Object> enrichCache = storeManager.enrich(message);
        assertEquals(result, enrichCache);
    }
}

