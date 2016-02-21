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
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.redborder.samza.util.constants.Dimension.*;

public class RadiusProcessor extends Processor<Map<String, Object>> {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_FLOW_OUTPUT_TOPIC);
    public final static String RADIUS_STORE = "radius";
    private static final String DATASOURCE = "rb_flow";
    private static final Logger log = LoggerFactory.getLogger(RadiusProcessor.class);

    private final List<String> toDruid = Arrays.asList(MARKET, MARKET_UUID, ORGANIZATION, ORGANIZATION_UUID,
            DEPLOYMENT, DEPLOYMENT_UUID, SENSOR_NAME, SENSOR_UUID, NAMESPACE, SERVICE_PROVIDER, SERVICE_PROVIDER_UUID,
            NAMESPACE_UUID);

    private KeyValueStore<String, Long> countersStore;
    private KeyValueStore<String, Long> flowsNumber;
    private KeyValueStore<String, Map<String, Object>> storeRadius;
    private Counter messagesCounter;
    private StoreManager storeManager;
    private EnrichManager enrichManager;
    private Map<String, Map<String, Object>> mobileCodeData;
    private Pattern pattern = Pattern.compile("^([a-fA-F0-9][a-fA-F0-9][:\\-][a-fA-F0-9][a-fA-F0-9][:\\-][a-fA-F0-9][a-fA-F0-9][:\\-][a-fA-F0-9][a-fA-F0-9][:\\-][a-fA-F0-9][a-fA-F0-9][:\\-][a-fA-F0-9][a-fA-F0-9])[:\\-]((.*))?");
    Pattern mobilePattern = Pattern.compile("[0-9A-Z]\\w+@wlan.mnc([0-9]\\w+).mcc([0-9]\\w+).[A-Za-z0-9]\\w+.[A-Za-z0-9]\\w+");

    public RadiusProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        this.messagesCounter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
        this.storeManager = storeManager;
        this.enrichManager = enrichManager;
        storeRadius = storeManager.getStore(RADIUS_STORE);
        countersStore = (KeyValueStore<String, Long>) context.getStore("counter");
        flowsNumber = (KeyValueStore<String, Long>) context.getStore("flows-number");

        try {
            mobileCodeData = new ObjectMapper().readValue(new File("/tmp/mobile_code.json"), Map.class);
        } catch (IOException e) {
            log.error("Error parser /tmp/mobile_code.json", e);
            mobileCodeData = new HashMap<>();
        }

        log.info("MobileCodeData [{}]", mobileCodeData);
    }

    @Override
    public String getName() {
        return "radius";
    }

    @Override
    public void process(String stream, Map<String, Object> message, MessageCollector collector) {
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

        String namespace = (String) message.get(NAMESPACE_UUID);
        String namespace_id = namespace == null ? "" : namespace;

        Object timestamp = message.get(TIMESTAMP);

        if (clientMac != null) {
            clientMac = clientMac.replaceAll("-", ":").toLowerCase();
            toDruid.put(CLIENT_MAC, clientMac);

            for (String dimension : this.toDruid) {
                Object value = message.get(dimension);

                if (value != null) {
                    toDruid.put(dimension, value);
                }
            }

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
                Matcher matcher = mobilePattern.matcher(clientId);

                if (matcher.find()) {
                    String mobileCode = matcher.group(1) + matcher.group(2);
                    Map<String, Object> operator = mobileCodeData.get(mobileCode);
                    log.info("MobileCode [{}] Operator[{}]", mobileCode, operator);
                    if (operator != null) {
                        toCache.putAll(operator);
                    } else {
                        toCache.put(CLIENT_ID, clientId);
                    }
                } else {
                    log.info("Doesn't match: [{}]", clientId);
                    toCache.put(CLIENT_ID, clientId);
                }
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

            Map<String, Object> storeMessage = storeManager.enrich(toDruid);
            storeMessage.putAll(toDruid);
            Map<String, Object> enrichmentMessage = enrichManager.enrich(storeMessage);

            String datasource = DATASOURCE;
            String namespaceUUID = (String) enrichmentMessage.get(Dimension.NAMESPACE_UUID);

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
                enrichmentMessage.put("flows_count", flows);
            }

            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, enrichmentMessage));
        }
        this.messagesCounter.inc();
    }
}
