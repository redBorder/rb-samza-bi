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

import java.util.HashMap;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;

public class MerakiProcessor extends Processor<Map<String, Object>> {
    private static final Logger log = LoggerFactory.getLogger(MerakiProcessor.class);
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_FLOW_OUTPUT_TOPIC);
    final public static String MERAKI_STORE = "meraki";

    private KeyValueStore<String, Map<String, Object>> store;
    private Counter counter;

    @Override
    public String getName() {
        return "meraki";
    }

    public MerakiProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        store = storeManager.getStore(MERAKI_STORE);
        counter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        String clientMac = (String) message.get(CLIENT_MAC);
        String clientLatLong = (String) message.get(CLIENT_LATLNG);
        String clientMacVendor = (String) message.get(CLIENT_MAC_VENDOR);
        Integer clientRSSI = (Integer) message.get(CLIENT_RSSI_NUM);
        String wirelessStation = (String) message.get(WIRELESS_STATION);
        String clientOS = (String) message.get(CLIENT_OS);

        if(clientMac != null) {
            Map<String, Object> toCache = new HashMap<>();

            if (clientLatLong != null) {
                toCache.put(CLIENT_LATLNG, clientLatLong);
            }

            if (wirelessStation != null) {
                toCache.put(WIRELESS_STATION, wirelessStation);
            }

            if (clientMacVendor != null) {
                toCache.put(CLIENT_MAC_VENDOR, clientMacVendor);
            }

            if (clientRSSI != null) {
                toCache.put(CLIENT_RSSI_NUM, clientRSSI);
            }

            if (clientOS != null) {
                toCache.put(CLIENT_OS, clientOS);
            }

            store.put(clientMac, toCache);


            Map<String, Object> toDruid = new HashMap<>();
            toDruid.putAll(message);

            Map<String, Object> enrichmentEvent = enrichManager.enrich(toDruid);
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, enrichmentEvent));
        } else {
            log.warn("This event {} doesn't have client mac.", message);
        }
    }
}
