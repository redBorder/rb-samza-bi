package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.constants.Constants;
import net.redborder.samza.util.constants.Dimension;
import org.apache.samza.config.Config;
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
    private static final String DATASOURCE = "rb_flow";

    private final List<String> dimToCache = Arrays.asList(CLIENT_LATLNG, WIRELESS_STATION, CLIENT_MAC_VENDOR,
            CLIENT_RSSI_NUM, CLIENT_OS);

    private KeyValueStore<String, Map<String, Object>> store;
    private KeyValueStore<String, Long> countersStore;
    private KeyValueStore<String, Long> flowsNumber;

    @Override
    public String getName() {
        return "meraki";
    }

    public MerakiProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        store = storeManager.getStore(LOCATION_STORE);
        countersStore = (KeyValueStore<String, Long>) context.getStore("counter");
        flowsNumber = (KeyValueStore<String, Long>) context.getStore("flows-number");
    }

    @Override
    public void process(String stream, Map<String, Object> message, MessageCollector collector) {
        String clientMac = (String) message.get(CLIENT_MAC);

        if (clientMac != null) {
            Map<String, Object> toCache = new HashMap<>();

            for (String dimension : dimToCache) {
                Object value = message.get(dimension);
                if (value != null) {
                    toCache.put(dimension, value);
                }
            }
            Map<String, Object> toDruid = new HashMap<>();


            String rssiName;
            Integer rssi = (Integer) message.get(CLIENT_RSSI_NUM);

            if(message.containsKey(SRC)){
                toCache.put(DOT11STATUS, "ASSOCIATED");
                toDruid.put(DOT11STATUS, "ASSOCIATED");
            } else {
                toCache.put(DOT11STATUS, "PROBING");
                toDruid.put(DOT11STATUS, "PROBING");
            }

            if(rssi != null) {
                if (rssi == 0) {
                    toCache.put(CLIENT_RSSI, "unknown");
                    rssiName = "unknown";
                } else if (rssi <= -80) {
                    toCache.put(CLIENT_RSSI, "bad");
                    rssiName = "bad";
                } else if (rssi <= -70) {
                    toCache.put(CLIENT_RSSI, "average");
                    rssiName = "average";
                } else {
                    toCache.put(CLIENT_RSSI, "good");
                    rssiName = "good";
                }

                toCache.put(CLIENT_RSSI, rssiName);
                toDruid.put(CLIENT_RSSI, rssiName);
            }

            store.put(clientMac, toCache);

            toDruid.putAll(message);

            Map<String, Object> storeEnrichment = storeManager.enrich(toDruid);
            storeEnrichment.putAll(toDruid);
            Map<String, Object> enrichmentEvent = enrichManager.enrich(storeEnrichment);

            String datasource = DATASOURCE;
            String namespace = (String) enrichmentEvent.get(Dimension.NAMESPACE_UUID);

            if (namespace != null) {
                datasource = String.format("%s_%s", DATASOURCE, namespace);
            }

            Long counter = countersStore.get(datasource);

            if(counter == null){
                counter = 0L;
            }

            counter++;
            countersStore.put(datasource, counter);

            Long flows = flowsNumber.get(datasource);

            if(flows != null){
                enrichmentEvent.put("flows_count", flows);
            }

            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, clientMac, enrichmentEvent));
        } else {
            log.warn("This event {} doesn't have client mac.", message);
        }
    }
}
