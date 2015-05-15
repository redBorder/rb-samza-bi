package net.redborder.samza.processors;

import junit.framework.TestCase;
import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.MockMessageCollector;
import net.redborder.samza.util.MockTaskContext;
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

import static net.redborder.samza.util.constants.Dimension.*;

@RunWith(MockitoJUnitRunner.class)
public class FlowProcessorTest extends TestCase {
    static FlowProcessor flowProcessor;
    static StoreManager storeManager;
    static EnrichManager enrichManager;

    @Mock
    static Config config;

    static TaskContext context;
    static List<String> stores = new ArrayList<>();

    @BeforeClass
    public static void initTest() throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = new FileInputStream("src/main/config/enrichment.properties");
        properties.load(inputStream);

        context = new MockTaskContext();

        config = mock(Config.class);
        when(config.getList("redborder.stores", Collections.<String>emptyList())).thenReturn(stores);

        String storesListAsString = properties.getProperty("redborder.stores");
        for (String store : storesListAsString.split(",")) {
            stores.add(store);
            String storeKey = properties.getProperty("redborder.store." + store + ".key");
            when(config.get("redborder.store." + store + ".key", CLIENT_MAC)).thenReturn(storeKey);
        }

        storeManager = new StoreManager(config, context);
        enrichManager = new EnrichManager();
        flowProcessor = new FlowProcessor(storeManager, enrichManager, config, context);
    }

    @Test
    public void enrichmentCorrectly() {
        MockMessageCollector collector = new MockMessageCollector();
        Map<String, Object> expected = new HashMap<>();

        // The message that we will enrich
        Map<String, Object> message = new HashMap<>();
        message.put(CLIENT_MAC, "00:00:00:00:00:00");
        message.put(BYTES, 23L);
        message.put(PKTS, 2L);
        message.put(TIMESTAMP, Long.valueOf(1429088471L));
        expected.putAll(message);
        expected.put(DURATION, 0L);

        for (String store : stores) {
            Map<String, Object> cache = new HashMap<>();
            // The data that will be in each cache ...
            cache.put("column_" + store, "value_" + store);
            cache.put("column2_" + store, "value2" + store);
            storeManager.getStore(store).put("00:00:00:00:00:00", cache);
            // ... will end in the expected message too
            expected.put("column_" + store, "value_" + store);
            expected.put("column2_" + store, "value2" + store);
        }

        // Send the message
        flowProcessor.process(message, collector);

        // Lets see if the collector received it correctly
        Map<String, Object> result = collector.getResult().get(0);
        assertEquals(expected, result);
    }

    @Test
    public void enrichmentCorrectlyUsingNamespaceId() {
        MockMessageCollector collector = new MockMessageCollector();
        Map<String, Object> expected = new HashMap<>();

        String namespace_id = "tenant_A";

        // The message that we will enrich
        Map<String, Object> message = new HashMap<>();
        message.put(CLIENT_MAC, "00:00:00:00:00:00");
        message.put(BYTES, 23L);
        message.put(PKTS, 2L);
        message.put(NAMESPACE_ID, namespace_id);
        message.put(TIMESTAMP, Long.valueOf(1429088471L));
        expected.putAll(message);
        expected.put(DURATION, 0L);

        for (String store : stores) {
            Map<String, Object> cache = new HashMap<>();
            // The data that will be in each cache ...
            cache.put("column_" + store + namespace_id, "value_" + store);
            cache.put("column2_" + store + namespace_id, "value2" + store);
            storeManager.getStore(store).put("00:00:00:00:00:00" + namespace_id, cache);
            // ... will end in the expected message too
            expected.put("column_" + store + namespace_id, "value_" + store);
            expected.put("column2_" + store + namespace_id, "value2" + store);
        }

        // Send the message
        flowProcessor.process(message, collector);

        // Lets see if the collector received it correctly
        Map<String, Object> result = collector.getResult().get(0);
        assertEquals(expected, result);
    }

    @Test
    public void enrichmentWithOverride() {
        MockMessageCollector collector = new MockMessageCollector();
        Map<String, Object> expected = new HashMap<>();

        // This is practically the same case than the normal enrichment, but
        // this time every store have the same columns, therefore the final message
        // will only contain the columns that are saved in the last of the stores

        // The message that we will enrich
        Map<String, Object> message = new HashMap<>();
        message.put(CLIENT_MAC, "00:00:00:00:00:00");
        message.put(BYTES, 23L);
        message.put(PKTS, 2L);
        message.put(TIMESTAMP, Long.valueOf(1429088471L));
        expected.putAll(message);
        expected.put(DURATION, 0L);

        for (String store : stores) {
            Map<String, Object> cache = new HashMap<>();
            // The data that will be in each cache ...
            cache.put("column", "value_" + store);
            cache.put("column2", "value2_" + store);
            storeManager.getStore(store).put("00:00:00:00:00:00", cache);
            // ... will end in the expected message too
            expected.put("column", "value_" + store);
            expected.put("column2", "value2_" + store);
        }

        // Send the message
        flowProcessor.process(message, collector);

        // Lets see if the collector received it correctly
        Map<String, Object> result = collector.getResult().get(0);
        assertEquals(expected, result);
    }

    @Test
    public void checkName() {
        assertEquals("flow", flowProcessor.getName());
    }
}

