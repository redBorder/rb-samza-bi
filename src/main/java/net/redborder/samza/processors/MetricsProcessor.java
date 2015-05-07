package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.constants.Constants;
import org.apache.samza.config.Config;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetricsProcessor extends Processor<Object> {
    private static final Logger log = LoggerFactory.getLogger(MetricsProcessor.class);
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.MONITOR_TOPIC);

    public MetricsProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
    }

    @Override
    public String getName() {
        return "metrics";
    }

    @Override
    public void process(Object message, MessageCollector collector) {
        if (message instanceof Map) {
            Map<String, Object> messageMap = (Map<String, Object>) message;
            if (messageMap.containsKey("asMap")) {
                Map<String, Object> asMap = (Map<String, Object>) messageMap.get("asMap");
                Map<String, Object> metrics = (Map<String, Object>) asMap.get("metrics");
                Map<String, Object> header = (Map<String, Object>) asMap.get("header");

                for (Map.Entry<String, Object> classEntry : metrics.entrySet()) {
                    Map<String, Object> contents = (Map<String, Object>) classEntry.getValue();

                    for (Map.Entry<String, Object> contentEntry : contents.entrySet()) {
                        Map<String, Object> toDruid = new HashMap<>();
                        String className = classEntry.getKey().toLowerCase();
                        if (className.contains("net.redborder")) {
                            String[] classNameSeparated = className.split("\\.");

                            if (classNameSeparated.length != 0) {
                                className = classNameSeparated[classNameSeparated.length - 1];
                            }

                            Long timestamp = (Long) header.get("time");
                            timestamp = timestamp / 1000L;
                            String taskPartition = (String) header.get("source");
                            String partition = taskPartition.split(" ")[1];
                            toDruid.put("sensor_name", header.get("container-name") + "-" + partition);
                            toDruid.put("type", className);
                            toDruid.put("monitor", className + "_" + contentEntry.getKey());
                            toDruid.put("value", contentEntry.getValue());
                            toDruid.put("timestamp", timestamp);

                            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, toDruid));
                        }
                    }
                }
            }
        } else if (message instanceof List) {
            List<Map<String, Object>> messageList = (ArrayList<Map<String, Object>>) message;

            for (Map<String, Object> metric : messageList) {
                Map<String, Object> toDruid = new HashMap<>();

                String host = metric.get("host").toString();
                toDruid.put("timestamp", new DateTime(metric.get("timestamp")).withZone(DateTimeZone.UTC).getMillis() / 1000);
                toDruid.put("sensor_name", host);
                toDruid.put("monitor", metric.get("service") + "/" + metric.get("metric"));
                toDruid.put("value", metric.get("value"));

                if (metric.get("service").toString().equals("historical")) {
                    if (metric.get("user1") != null)
                        toDruid.put("data_source", metric.get("user1"));
                    if (metric.get("user2") != null)
                        toDruid.put("tier", metric.get("user2"));
                } else if (metric.get("service").toString().equals("realtime")){
                    if (metric.get("user1") != null)
                        toDruid.put("data_source", metric.get("user1"));
                    if (metric.get("user2") != null)
                        toDruid.put("data_source", metric.get("user2"));
                }else if (metric.get("service").toString().equals("coordinator")){
                    if (metric.get("user1") != null) {
                        if (metric.get("metric").equals("coordinator/segment/size") || metric.get("metric").equals("coordinator/segment/count")) {
                            toDruid.put("data_source", metric.get("user1"));
                        } else {
                            toDruid.put("historical_server", metric.get("user1"));
                        }
                    }
                }  else if (metric.get("service").toString().equals("overlord") || metric.get("service").toString().equals("indexer")){
                    if (metric.get("user2") != null)
                        toDruid.put("data_source", metric.get("user2"));
                    if (metric.get("user3") != null)
                        toDruid.put("task_status", metric.get("user3"));
                    if (metric.get("user4") != null)
                        toDruid.put("task_type", metric.get("user4"));
                    if (metric.get("user5") != null)
                        toDruid.put("segment_interval", metric.get("user5"));
                }

                if(metric.get("metric") != null && metric.get("metric").toString().contains("query")){
                    if (metric.get("user2") != null)
                        toDruid.put("data_source", metric.get("user2"));
                    if (metric.get("user3") != null)
                        toDruid.put("query_context_dump", metric.get("user3"));
                    if (metric.get("user4") != null)
                        toDruid.put("query_type", metric.get("user4"));
                    if (metric.get("user5") != null)
                        toDruid.put("query_interval", metric.get("user5"));
                    if (metric.get("user8") != null)
                        toDruid.put("query_id", metric.get("user8"));
                    if (metric.get("user9") != null)
                        toDruid.put("query_duration", metric.get("user9"));
                }

                collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, toDruid));
            }
        } else {
            log.info("Unrecognized object type on metrics");
        }
    }
}
