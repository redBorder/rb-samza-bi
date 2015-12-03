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

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.redborder.samza.util.constants.Dimension.*;

public class RadiusProcessor extends Processor<Map<String, Object>> {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_FLOW_OUTPUT_TOPIC);
    public final static String RADIUS_STORE = "radius";
    private static final Logger log = LoggerFactory.getLogger(RadiusProcessor.class);

    private KeyValueStore<String, Map<String, Object>> storeRadius;
    private Counter messagesCounter;
    private StoreManager storeManager;
    private EnrichManager enrichManager;
    private Pattern pattern = Pattern.compile("^([a-fA-F0-9][a-fA-F0-9][:\\-][a-fA-F0-9][a-fA-F0-9][:\\-][a-fA-F0-9][a-fA-F0-9][:\\-][a-fA-F0-9][a-fA-F0-9][:\\-][a-fA-F0-9][a-fA-F0-9][:\\-][a-fA-F0-9][a-fA-F0-9])[:\\-]((.*))?");


    public RadiusProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        this.messagesCounter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
        this.storeManager = storeManager;
        this.enrichManager = enrichManager;
        storeRadius = storeManager.getStore(RADIUS_STORE);
    }

    @Override
    public String getName() {
        return "radius";
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        Map<String, Object> toCache = new HashMap<>();
        Map<String, Object> toDruid = new HashMap<>();


        String sensorIP = (String) message.get(PACKET_SRC_IP_ADDRESS);
        String clientId = (String) message.get(USER_NAME_RADIUS);
        String operatorName = (String) message.get(OPERATOR_NAME);
        String wirelessId = (String) message.get(AIRESPACE_WLAN_ID);
        String clientMac = (String) message.get(CALLING_STATION_ID);
        String clientConnection = (String) message.get(ACCT_STATUS_TYPE);
        String wirelessStationSSID = (String) message.get(CALLED_STATION_ID);
        Map<String, Object> enrichment = (Map<String, Object>) message.get("enrichment");

        String namespace = null;

        if (enrichment != null) {
            namespace = (String) enrichment.get(NAMESPACE_UUID);
        }

        String namespace_id = namespace == null ? "" : namespace;

        Object timestamp = message.get(TIMESTAMP);

        if (clientMac != null) {
            clientMac = clientMac.replaceAll("-", ":").toLowerCase();
            toDruid.put(CLIENT_MAC, clientMac);

            if (enrichment != null) {
                toDruid.putAll(enrichment);
            }

            if (timestamp != null) {
                toDruid.put(TIMESTAMP, timestamp);
            } else {
                toDruid.put(TIMESTAMP, System.currentTimeMillis() / 1000);
            }

            if (sensorIP != null) {
                toDruid.put(SENSOR_IP, sensorIP);
            }
            if (clientId != null) {
                toCache.put(CLIENT_ID, clientId);
            }
            if (operatorName != null) {
                toCache.put(WIRELESS_OPERATOR, operatorName);
            }
            if (wirelessId != null) {
                toCache.put(WIRELESS_ID, wirelessId);
            }
            if (wirelessStationSSID != null) {
                Matcher matcher = pattern.matcher(wirelessStationSSID);
                if (matcher.find()) {
                    if (matcher.groupCount() == 3) {
                        String mac = matcher.group(1).replace("-", ":").toLowerCase();
                        toCache.put(WIRELESS_STATION, mac);
                        toCache.put(WIRELESS_ID, matcher.group(2));
                    } else if (matcher.groupCount() == 2) {
                        String mac = matcher.group(1).replace("-", ":").toLowerCase();
                        toCache.put(WIRELESS_STATION, mac);
                    }
                }
            }

            if (clientConnection != null) {
                toDruid.put(CLIENT_ACCOUNTING_TYPE, clientConnection.toLowerCase());
                if (clientConnection.equals("Stop")) {
                    //storeRadius.delete(clientMac + namespace_id);
                    log.debug("REMOVE  client: {} - namesapce: {} - contents: " + toCache, clientMac, namespace_id);

                } else {
                    storeRadius.put(clientMac + namespace_id, toCache);
                    log.debug("PUT  client: {} - namesapce: {} - contents: " + toCache, clientMac, namespace_id);

                }
            } else {
                storeRadius.put(clientMac + namespace_id, toCache);
                log.debug("PUT  client: {} - namesapce: {} - contents: " + toCache, clientMac, namespace_id);
            }

            toDruid.put(BYTES, 0);
            toDruid.put(PKTS, 0);
            toDruid.put(TYPE, "radius");
            toDruid.putAll(toCache);

            Map<String, Object> enrichmentMessage = enrichManager.enrich(toDruid);
            Map<String, Object> storeMessage = storeManager.enrich(enrichmentMessage);

            storeMessage.putAll(toDruid);
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, storeMessage));
        }
        this.messagesCounter.inc();
    }
}
