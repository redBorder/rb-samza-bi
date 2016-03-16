package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.constants.Constants;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;

public class PmsProcessor extends Processor<Map<String, Object>> {
    private static final Logger log = LoggerFactory.getLogger(PmsProcessor.class);
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_FLOW_OUTPUT_TOPIC);
    public final static String PMS_STORE = "pms";

    private KeyValueStore<String, Map<String, Object>> storePms;
    private Counter messagesCounter;
    private StoreManager storeManager;

    public PmsProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        this.messagesCounter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
        this.storeManager = storeManager;
        this.enrichManager = enrichManager;
        storePms = storeManager.getStore(PMS_STORE);
    }

    @Override
    public void process(String stream, Map<String, Object> message, MessageCollector collector) {
        Map<String, Object> enrichmentMsg = storeManager.enrich(message);

        String clientMac = (String) enrichmentMsg.get(CLIENT_MAC);
        String guestName = (String) enrichmentMsg.get(GUEST_NAME);
        Object namespace = enrichmentMsg.get(NAMESPACE_UUID);
        String clientGender = (String) enrichmentMsg.get(CLIENT_GENDER);
        String clientAuth = (String) enrichmentMsg.get(AUTH_TYPE);
        String staffName = (String) enrichmentMsg.get(STAFF_NAME);
        String loyaltyStatus = (String) enrichmentMsg.get(CLIENT_LOYALITY);
        String vipStatus = (String) enrichmentMsg.get(CLIENT_VIP);


        String namespace_id = namespace == null ? "" : namespace.toString();

        Map<String, Object> toCache = new HashMap<>();

        if(guestName != null) {
            toCache.put(CLIENT_FULLNAME, guestName);
        }

        if(staffName != null) {
            toCache.put(CLIENT_FULLNAME, staffName);
        }

        if(clientGender != null) {
            toCache.put(CLIENT_GENDER, clientGender);
        }

        if(clientAuth != null) {
            toCache.put(CLIENT_AUTH_TYPE, clientAuth);
        }

        if(vipStatus != null) {
            toCache.put(CLIENT_VIP, vipStatus);
        }

        if(loyaltyStatus != null) {
            toCache.put(CLIENT_LOYALITY, loyaltyStatus);
        }

        if(!toCache.isEmpty()) {
            storePms.put(clientMac + namespace_id, toCache);
        }
    }

    @Override
    public String getName() {
        return "pms";
    }
}
