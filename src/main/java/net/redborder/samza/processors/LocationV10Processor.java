package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.constants.Constants;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;

public class LocationV10Processor extends Processor<Map<String, Object>> {
    private static final Logger log = LoggerFactory.getLogger(LocationV10Processor.class);
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_FLOW_OUTPUT_TOPIC);
    final public static String LOCATION_STORE = "location";

    private KeyValueStore<String, Map<String, Object>> store;
    private Map<Integer, String> cache;
    private Counter counter;

    public LocationV10Processor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        store = storeManager.getStore(LOCATION_STORE);

        cache = new HashMap<>();
        cache.put(0, "IDLE");
        cache.put(1, "AAA_PENDING");
        cache.put(2, "AUTHENTICATED");
        cache.put(3, "ASSOCIATED");
        cache.put(4, "POWERSAVE");
        cache.put(5, "DISASSOCIATED");
        cache.put(6, "TO_BE_DELETED");
        cache.put(7, "PROBING");
        cache.put(8, "BLACK_LISTED");
        cache.put(256, "WAIT_AUTHENTICATED");
        cache.put(257, "WAIT_ASSOCIATED");

        counter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
    }

    @Override
    public String getName() {
        return "locv10";
    }

    @Override
    @SuppressWarnings("unchecked cast")
    public void process(Map<String, Object> message, MessageCollector collector) {
        List<Map<String, Object>> notifications = (List<Map<String, Object>>) message.get(LOC_NOTIFICATIONS);

        if (notifications != null) {
            for (Map<String, Object> notification : notifications) {
                String notificationType = (String) notification.get(LOC_NOTIFICATION_TYPE);
                if (notificationType == null) notificationType = "null";

                if (notificationType.equals("association")) {
                    log.trace("Mse10 event this event is a association, emitting " + notification.size());
                    processAssociation(message, collector);
                } else if (notificationType.equals("locationupdate")) {
                    log.trace("Mse10 event this event is a locationupdate, emitting " + notification.size());
                    processLocationUpdate(message, collector);
                } else {
                    log.warn("MSE version 10 notificationType is unknown: " + notificationType);
                }
                counter.inc();
            }
        }
    }

    @SuppressWarnings("unchecked cast")
    public void processAssociation(Map<String, Object> message, MessageCollector collector) {
        try {
            List<Map<String, Object>> messages = (ArrayList) message.get("notifications");

            for (Map<String, Object> msg : messages) {
                log.trace("Processing mse10Association " + msg);
                Map<String, Object> toCache = new HashMap<>();
                Map<String, Object> toDruid = new HashMap<>();

                String clientMac = (String) msg.get(LOC_DEVICEID);
                String namespace_id = msg.get(NAMESPACE_UUID) == null ? "" : (String) msg.get(NAMESPACE_UUID);

                if (msg.get(LOC_SSID) != null)
                    toCache.put(WIRELESS_ID, msg.get(LOC_SSID));

                if (msg.get(LOC_BAND) != null)
                    toCache.put(NMSP_DOT11PROTOCOL, msg.get(LOC_BAND));

                if (msg.get(LOC_STATUS) != null) {
                    Integer msgStatus = (Integer) msg.get(LOC_STATUS);
                    toCache.put(DOT11STATUS, cache.get(msgStatus));
                }

                if (msg.get(LOC_AP_MACADDR) != null)
                    toCache.put(WIRELESS_STATION, msg.get(LOC_AP_MACADDR));

                if (msg.get(LOC_USERNAME) != null && !msg.get(LOC_USERNAME).equals(""))
                    toCache.put(CLIENT_ID, msg.get(LOC_USERNAME));

                toDruid.putAll(toCache);
                toDruid.put(SENSOR_NAME, msg.get(LOC_SUBSCRIPTION_NAME));
                toDruid.put(CLIENT_MAC, clientMac);
                toDruid.put(TIMESTAMP, ((Long) msg.get(TIMESTAMP)) / 1000L);
                toDruid.put(BYTES, 0);
                toDruid.put(PKTS, 0);
                toDruid.put(TYPE, "mse10");

                store.put(clientMac + namespace_id, toCache);
                collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, toDruid));
            }
        } catch (Exception ex) {
            log.warn("MSE10 association event dropped: " + message, ex);
        }
    }

    @SuppressWarnings("unchecked cast")
    public void processLocationUpdate(Map<String, Object> message, MessageCollector collector) {
        try {
            List<Map<String, Object>> messages = (ArrayList) message.get("notifications");

            for (Map<String, Object> msg : messages) {
                log.trace("Processing mse10LocationUpdate " + msg);

                Map<String, Object> toCache = new HashMap<>();
                Map<String, Object> toDruid = new HashMap<>();

                String namespace_id = msg.get(NAMESPACE_UUID) == null ? "" : (String) msg.get(NAMESPACE_UUID);
                String clientMac = (String) msg.get(LOC_DEVICEID);
                String locationMapHierarchy = (String) msg.get(LOC_MAP_HIERARCHY_V10);

                if (msg.get(LOC_AP_MACADDR) != null)
                    toCache.put(WIRELESS_STATION, msg.get(LOC_AP_MACADDR));

                if (locationMapHierarchy != null) {
                    String[] locations = locationMapHierarchy.split(">");

                    if (locations.length >= 1)
                        toCache.put(CAMPUS, locations[0]);
                    if (locations.length >= 2)
                        toCache.put(BUILDING, locations[1]);
                    if (locations.length >= 3)
                        toCache.put(FLOOR, locations[2]);
                    if (locations.length >= 4)
                        toCache.put(ZONE, locations[3]);
                }

                Map<String, Object> assocCache = store.get(clientMac + namespace_id);

                if (assocCache != null) {
                    toCache.putAll(assocCache);
                } else {
                    toCache.put(DOT11STATUS, "PROBING");
                }

                toDruid.putAll(toCache);
                toDruid.put(SENSOR_NAME, msg.get(LOC_SUBSCRIPTION_NAME));

                if (msg.containsKey(TIMESTAMP)) {
                    toDruid.put(TIMESTAMP, ((Long) msg.get(TIMESTAMP)) / 1000L);
                } else {
                    toDruid.put(TIMESTAMP, System.currentTimeMillis() / 1000L);
                }

                toDruid.put(BYTES, 0);
                toDruid.put(PKTS, 0);
                toDruid.put(CLIENT_MAC, clientMac);
                toDruid.put(TYPE, "mse10");

                if (!namespace_id.equals(""))
                    toDruid.put(NAMESPACE_UUID, namespace_id);

                store.put(clientMac + namespace_id, toCache);

                Map<String, Object> enrichmentEvent = enrichManager.enrich(toDruid);
                Map<String, Object> storeEnrichment = storeManager.enrich(enrichmentEvent);

                collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, storeEnrichment));
            }
        } catch (Exception ex) {
            log.warn("MSE10 locationUpdate event dropped: " + message, ex);
        }
    }
}
