package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.redborder.samza.util.constants.Dimension.*;


import java.util.HashMap;
import java.util.Map;

public class DwellProcessor extends Processor<Map<String, Object>> {
    public final static String DWELL_STORE = "dwell";
    private static final Logger log = LoggerFactory.getLogger(DwellProcessor.class);

    private KeyValueStore<String, Map<String, Object>> storeDwell;

    private Long maxTime;

    public DwellProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        storeDwell = storeManager.getStore(DWELL_STORE);
        maxTime = config.getLong("net.redborder.dwell.maxTime", 3600);
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        Object realTimestamp = message.get(TIMESTAMP);
        Long timestamp = System.currentTimeMillis() / 1000;
        String client = (String) message.get(CLIENT_MAC);

        if (realTimestamp == null) {
            Map<String, Object> mseEventContent = (Map<String, Object>) message.get(LOC_STREAMING_NOTIFICATION);
            if (message.containsKey(LOC_STREAMING_NOTIFICATION)) {
                String dateString = (String) mseEventContent.get(TIMESTAMP);
                Map<String, Object> location = (Map<String, Object>) mseEventContent.get(LOC_LOCATION);
                client = (String) location.get(LOC_MACADDR);
                if (dateString != null) {
                    timestamp = new DateTime(dateString).withZone(DateTimeZone.UTC).getMillis() / 1000;
                }
            }
        } else {
            if(realTimestamp instanceof String) {
                timestamp = Long.parseLong((String) realTimestamp);
            } else if(realTimestamp instanceof Integer){
                Integer t = (Integer) realTimestamp;
                timestamp = t.longValue();
            } else if(realTimestamp instanceof  Long){
                timestamp = (Long) realTimestamp;
            }
        }

        if(client != null) {
            String namespace = (String) message.get(NAMESPACE_UUID);
            String namespace_id = namespace == null ? "" : namespace.toString();
            String key = client + namespace_id;

            Map<String, Object> lastValue = storeDwell.get(key);

            if (lastValue == null) {
                lastValue = new HashMap<>();
                lastValue.put("first_seen", timestamp);
                lastValue.put("last_seen", timestamp);
                lastValue.put("window", 0);
            } else {
                Object firstSeenPre = lastValue.get("first_seen");
                Object lastSeenPre = lastValue.get("last_seen");
                Long firstSeen = 0L;
                Long lastSeen = 0L;

                if (firstSeenPre instanceof Integer) {
                    firstSeen = ((Integer) firstSeenPre).longValue();
                } else if (firstSeenPre instanceof Long) {
                    firstSeen = (Long) firstSeenPre;
                }

                if (lastSeenPre instanceof Integer) {
                    lastSeen = ((Integer) lastSeenPre).longValue();
                } else if (lastSeenPre instanceof Long) {
                    lastSeen = (Long) lastSeenPre;
                }

                if (timestamp - lastSeen > maxTime) {
                    lastValue.put("first_seen", timestamp);
                    lastValue.put("last_seen", timestamp);
                    lastValue.put("window", 0);
                } else {
                    lastValue.put("first_seen", firstSeen);
                    lastValue.put("last_seen", timestamp);
                    Long window = (timestamp - firstSeen) / 60;

                    lastValue.put("window", window);
                }

            }

            log.debug("KEY: {} LASTVALUE: {} " + message, key, lastValue);
            storeDwell.put(key, lastValue);
        }
    }

    @Override
    public String getName() {
        return "dwell";
    }
}
