package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.constants.Constants;
import net.redborder.samza.util.constants.Dimension;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static net.redborder.samza.util.constants.Dimension.*;
import static net.redborder.samza.util.constants.DimensionValue.NMSP_TYPE_INFO;
import static net.redborder.samza.util.constants.DimensionValue.NMSP_TYPE_MEASURE;

public class NmspProcessor extends Processor<Map<String, Object>> {
    private static final Logger log = LoggerFactory.getLogger(NmspProcessor.class);
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_LOC_OUTPUT_TOPIC);
    final static String NMSP_STORE_MEASURE = "nmsp-measure";
    final static String NMSP_STORE_INFO = "nmsp-info";
    private static final String DATASOURCE = "rb_location";

    private final List<String> toCacheInfo = Arrays.asList(WIRELESS_STATION, WIRELESS_CHANNEL, WIRELESS_ID, NMSP_DOT11PROTOCOL);
    private final List<String> toDruid = Arrays.asList(MARKET, MARKET_UUID, ORGANIZATION, ORGANIZATION_UUID,
            DEPLOYMENT, DEPLOYMENT_UUID, SENSOR_NAME, SENSOR_UUID, NAMESPACE, SERVICE_PROVIDER, SERVICE_PROVIDER_UUID);

    private KeyValueStore<String, Map<String, Object>> storeMeasure;
    private KeyValueStore<String, Map<String, Object>> storeInfo;
    private KeyValueStore<String, Long> countersStore;
    private KeyValueStore<String, Long> flowsNumber;

    private Counter messagesCounter;
    private StoreManager storeManager;
    private EnrichManager enrichManager;

    private Integer rssiLimit;

    public NmspProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        this.messagesCounter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
        this.storeManager = storeManager;
        this.enrichManager = enrichManager;
        storeMeasure = storeManager.getStore(NMSP_STORE_MEASURE);
        storeInfo = storeManager.getStore(NMSP_STORE_INFO);
        countersStore = (KeyValueStore<String, Long>) context.getStore("counter");
        flowsNumber = (KeyValueStore<String, Long>) context.getStore("flows-number");
        rssiLimit = config.getInt("redborder.rssiLimit.db", -80);
    }

    @Override
    public String getName() {
        return "nmsp";
    }

    @Override
    public void process(String stream, Map<String, Object> message, MessageCollector collector) {
        Map<String, Object> toCache = new HashMap<>();
        Map<String, Object> toDruid = new HashMap<>();

        String type = (String) message.get(TYPE);
        String mac = (String) message.get(CLIENT_MAC);

        Object namespace = message.get(NAMESPACE_UUID);
        String namespace_id = namespace == null ? "" : namespace.toString();

        if (type != null && type.equals(NMSP_TYPE_MEASURE)) {
            List<String> apMacs = (List<String>) message.get(NMSP_AP_MAC);
            List<Integer> clientRssis = (List<Integer>) message.get(NMSP_RSSI);
            message.remove(type);

            if (clientRssis != null && apMacs != null && !apMacs.isEmpty() && !clientRssis.isEmpty()) {
                Integer rssi = Collections.max(clientRssis);
                String apMac = apMacs.get(clientRssis.indexOf(rssi));

                String rssiName;

                if (rssi == 0) {
                    toCache.put(CLIENT_RSSI, "unknown");
                    rssiName = "unknown";
                } else if (rssi <= -85) {
                    toCache.put(CLIENT_RSSI, "bad");
                    rssiName = "bad";
                } else if (rssi <= -80) {
                    toCache.put(CLIENT_RSSI, "low");
                    rssiName = "low";
                } else if (rssi <= -70) {
                    toCache.put(CLIENT_RSSI, "medium");
                    rssiName = "medium";
                } else if (rssi <= -60) {
                    toCache.put(CLIENT_RSSI, "good");
                    rssiName = "good";
                } else {
                    toCache.put(CLIENT_RSSI, "excelent");
                    rssiName = "excelent";
                }

                if (rssi == 0) {
                    toCache.put(CLIENT_PROFILE, "hard");
                } else if (rssi <= -75) {
                    toCache.put(CLIENT_PROFILE, "soft");
                } else if (rssi <= -65) {
                    toCache.put(CLIENT_PROFILE, "medium");
                } else {
                    toCache.put(CLIENT_PROFILE, "hard");
                }

                Map<String, Object> infoCache = storeInfo.get(mac + namespace_id);
                String dot11Status = "PROBING";

                if (infoCache == null) {
                    toCache.put(CLIENT_RSSI_NUM, rssi);
                    toCache.put(WIRELESS_STATION, apMac);
                    toCache.put(NMSP_DOT11STATUS, "ASSOCIATED");
                    dot11Status = "PROBING";
                } else {
                    Integer last_seen = (Integer) infoCache.get("last_seen");
                    if ((last_seen + 3600) > (System.currentTimeMillis() / 1000)) {
                        String apAssociated = (String) infoCache.get(WIRELESS_STATION);
                        if (apMacs.contains(apAssociated)) {
                            Integer rssiAssociated = clientRssis.get(apMacs.indexOf(apAssociated));
                            toCache.put(CLIENT_RSSI_NUM, rssiAssociated);
                            toCache.putAll(infoCache);
                            dot11Status = "ASSOCIATED";
                        } else {
                            toDruid = null;
                        }
                    } else {
                        storeInfo.delete(mac + namespace_id);
                        toCache.put(CLIENT_RSSI_NUM, rssi);
                        toCache.put(WIRELESS_STATION, apMac);
                        toCache.put(NMSP_DOT11STATUS, "ASSOCIATED");
                        dot11Status = "PROBING";
                    }
                }

                if (toDruid != null) {
                    for (String dimension : this.toDruid) {
                        Object value = message.get(dimension);

                        if (value != null) {
                            toDruid.put(dimension, value);
                        }
                    }

                    toDruid.put(TYPE, "nmsp-measure");
                    toDruid.put(CLIENT_MAC, mac);
                    toDruid.putAll(toCache);
                    toDruid.put(NMSP_DOT11STATUS, dot11Status);
                    toDruid.put(CLIENT_RSSI_NUM, rssi);
                    toDruid.put(CLIENT_RSSI, rssiName);

                    Object timestamp = message.get(TIMESTAMP);

                    if (timestamp == null) {
                        timestamp = System.currentTimeMillis() / 1000;
                    }

                    toDruid.put("timestamp", timestamp);

                    if (!namespace_id.equals(""))
                        toDruid.put(NAMESPACE_UUID, namespace_id);

                    storeMeasure.put(mac + namespace_id, toCache);

                    Map<String, Object> storeEnrichment = storeManager.enrich(toDruid);
                    storeEnrichment.putAll(toDruid);
                    Map<String, Object> enrichmentEvent = enrichManager.enrich(storeEnrichment);

                    String datasource = DATASOURCE;
                    Object namespaceUUID = enrichmentEvent.get(Dimension.NAMESPACE_UUID);

                    if (namespaceUUID != null) {
                        datasource = String.format("%s_%s", DATASOURCE, namespaceUUID);
                    }

                    Long counter = countersStore.get(datasource);

                    if (counter == null) {
                        counter = 0L;
                    }

                    counter++;
                    countersStore.put(datasource, counter);

                    Long flows = flowsNumber.get(datasource);

                    if (flows != null) {
                        enrichmentEvent.put("flows_count", flows);
                    }

                    if (rssi >= rssiLimit || dot11Status.equals("ASSOCIATED")) {
                        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, enrichmentEvent));
                    }
                }
            }
        } else if (type != null && type.equals(NMSP_TYPE_INFO)) {
            Object vlan = message.remove(NMSP_VLAN_ID);
            message.remove(type);

            if (vlan != null) {
                toCache.put(SRC_VLAN, vlan);
            }

            Integer timestamp;

            if (message.get("timestamp") != null) {
                timestamp = Integer.valueOf(String.valueOf(message.get("timestamp")));
            } else {
                timestamp = Long.valueOf(System.currentTimeMillis() / 1000).intValue();
            }


            for (String dimension : toCacheInfo) {
                Object value = message.get(dimension);
                if (value != null) {
                    toCache.put(dimension, value);
                }
            }

            toCache.put("last_seen", timestamp);
            toCache.put(NMSP_DOT11STATUS, "ASSOCIATED");

            toDruid.putAll(toCache);

            for (String dimension : this.toDruid) {
                Object value = message.get(dimension);

                if (value != null) {
                    toDruid.put(dimension, value);
                }
            }

            toDruid.put("timestamp", timestamp);
            toDruid.put(TYPE, "nmsp-info");

            if (!namespace_id.equals(""))
                toDruid.put(NAMESPACE_UUID, namespace_id);

            toDruid.put(CLIENT_MAC, mac);
            storeInfo.put(mac + namespace_id, toCache);

            Map<String, Object> storeEnrichment = storeManager.enrich(toDruid);
            storeEnrichment.putAll(toDruid);
            Map<String, Object> enrichmentEvent = enrichManager.enrich(storeEnrichment);

            String datasource = DATASOURCE;
            Object namespaceUUID = enrichmentEvent.get(Dimension.NAMESPACE_UUID);

            if (namespaceUUID != null) {
                datasource = String.format("%s_%s", DATASOURCE, namespaceUUID);
            }

            Long counter = countersStore.get(datasource);

            if (counter == null) {
                counter = 0L;
            }

            counter++;
            countersStore.put(datasource, counter);

            Long flows = flowsNumber.get(datasource);

            if (flows != null) {
                enrichmentEvent.put("flows_count", flows);
            }

            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, enrichmentEvent));
        }

        this.messagesCounter.inc();
    }
}
