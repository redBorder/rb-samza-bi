package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.constants.Constants;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;

public class MerakiProcessor extends Processor<Map<String, Object>> {
    private static final Logger log = LoggerFactory.getLogger(MerakiProcessor.class);
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_FLOW_OUTPUT_TOPIC);
    final public static String LOCATION_STORE = "location";

    private final List<String> dimToCache = Arrays.asList(CLIENT_LATLNG, WIRELESS_STATION, CLIENT_MAC_VENDOR,
            CLIENT_RSSI_NUM, CLIENT_OS);

    private KeyValueStore<String, Map<String, Object>> store;
    private Counter counter;

    @Override
    public String getName() {
        return "meraki";
    }

    public MerakiProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        store = storeManager.getStore(LOCATION_STORE);
        counter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        String clientMac = (String) message.get(CLIENT_MAC);

        if (clientMac != null) {
            Map<String, Object> toCache = new HashMap<>();

            for (String dimension : dimToCache) {
                Object value = message.get(dimension);
                if (value != null) {
                    toCache.put(dimension, value);
                }
            }

            store.put(clientMac, toCache);

            Map<String, Object> toDruid = new HashMap<>();
            toDruid.putAll(message);

            Map<String, Object> enrichmentEvent = enrichManager.enrich(toDruid);
            Map<String, Object> storeEnrichment = storeManager.enrich(enrichmentEvent);

            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, storeEnrichment));
        } else {
            log.warn("This event {} doesn't have client mac.", message);
        }
    }
}
