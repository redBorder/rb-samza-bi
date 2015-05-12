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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;
import static net.redborder.samza.util.constants.DimensionValue.NMSP_TYPE_INFO;
import static net.redborder.samza.util.constants.DimensionValue.NMSP_TYPE_MEASURE;

public class NmspProcessor extends Processor<Map<String, Object>> {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_FLOW_OUTPUT_TOPIC);
    public final static String NMSP_STORE_MEASURE = "nmsp-measure";
    public final static String NMSP_STORE_INFO = "nmsp-info";

    private KeyValueStore<String, Map<String, Object>> storeMeasure;
    private KeyValueStore<String, Map<String, Object>> storeInfo;
    private Counter messagesCounter;

    public NmspProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        this.messagesCounter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
        storeMeasure = storeManager.getStore(NMSP_STORE_MEASURE);
        storeInfo = storeManager.getStore(NMSP_STORE_INFO);
    }

    @Override
    public String getName() {
        return "nmsp";
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        Map<String, Object> toCache = new HashMap<>();
        Map<String, Object> toDruid = new HashMap<>();

        String type = (String) message.get(TYPE);
        String mac = (String) message.remove(CLIENT_MAC);
        Integer deployment = (Integer) message.get(Dimension.DEPLOYMENT_ID);
        String deployment_id =  deployment == null ? "" : deployment.toString();

        if (type != null && type.equals(NMSP_TYPE_MEASURE)) {
            List<String> apMacs = (List<String>) message.get(NMSP_AP_MAC);
            List<Integer> clientRssis = (List<Integer>) message.get(NMSP_RSSI);
            message.remove(type);

            if (clientRssis != null && apMacs != null && !apMacs.isEmpty() && !clientRssis.isEmpty()) {
                Integer rssi = Collections.max(clientRssis);
                String apMac = apMacs.get(clientRssis.indexOf(rssi));

                if (rssi == 0)
                    toCache.put(CLIENT_RSSI, "unknown");
                else if (rssi <= -85)
                    toCache.put(CLIENT_RSSI, "bad");
                else if (rssi <= -80)
                    toCache.put(CLIENT_RSSI, "low");
                else if (rssi <= -70)
                    toCache.put(CLIENT_RSSI, "medium");
                else if (rssi <= -60)
                    toCache.put(CLIENT_RSSI, "good");
                else
                    toCache.put(CLIENT_RSSI, "excelent");

                Map<String, Object> infoCache = storeInfo.get(mac + deployment_id);
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
                        storeInfo.delete(mac + deployment_id);
                        toCache.put(CLIENT_RSSI_NUM, rssi);
                        toCache.put(WIRELESS_STATION, apMac);
                        toCache.put(NMSP_DOT11STATUS, "ASSOCIATED");
                        dot11Status = "PROBING";
                    }
                }

                if(toDruid != null) {
                    toDruid.put(BYTES, 0);
                    toDruid.put(PKTS, 0);
                    toDruid.put(TYPE, "nmsp-measure");
                    toDruid.put(CLIENT_MAC, mac);
                    toDruid.putAll(toCache);
                    toDruid.put(NMSP_DOT11STATUS, dot11Status);

                    storeMeasure.put(mac + deployment_id, toCache);
                    toDruid.put("timestamp", System.currentTimeMillis() / 1000);
                    collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, toDruid));
                }
            }
        } else if (type != null && type.equals(NMSP_TYPE_INFO)) {
            Object vlan = message.remove(NMSP_VLAN_ID);
            message.remove(type);

            if (vlan != null) {
                toCache.put(SRC_VLAN, vlan);
            }

            Integer timestamp;

            if(message.get("timestamp") != null){
                timestamp = Integer.valueOf(String.valueOf(message.get("timestamp")));
            } else {
                timestamp = Long.valueOf(System.currentTimeMillis() / 1000).intValue();
            }

            toCache.putAll(message);
            toCache.put("last_seen", timestamp);
            toDruid.putAll(toCache);
            toDruid.put(BYTES, 0);
            toDruid.put(PKTS, 0);
            toDruid.put(TYPE, "nmsp-info");

            if (!deployment_id.equals(""))
                toDruid.put(DEPLOYMENT_ID, deployment_id);

            toDruid.put(CLIENT_MAC, mac);
            storeInfo.put(mac + deployment_id, toCache);
            toDruid.put("timestamp", timestamp);
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, toDruid));
        }

        this.messagesCounter.inc();
    }
}
