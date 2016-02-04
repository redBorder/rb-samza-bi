package net.redborder.samza.indexing.autoscaling;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AutoScalingManager {
    private static final Logger log = LoggerFactory.getLogger(AutoScalingManager.class);
    private Integer window_time;

    private Map<String, Long> eventsState = new ConcurrentHashMap<>();
    private Map<String, Map<String, Object>> dataSourceState = new ConcurrentHashMap<>();
    private Map<String, Integer> tiersLimit = new ConcurrentHashMap<>();
    private Map<String, Integer> currentPartitions = new HashMap<>();

    private static Double upPercent = 0.80;
    private static Double downPercent = 0.20;
    private static long eventsPerTask = 5000;

    public AutoScalingManager(Config config) {
        eventsPerTask = config.getLong("redborder.indexing.eventsPerTask", 5000);
        upPercent = config.getDouble("redborder.indexing.upPercent", 0.80);
        downPercent = config.getDouble("redborder.indexing.downPercent", 0.20);
        window_time = config.getInt("task.window.ms", 60000) / 1000;
        tiersLimit.put("gold", 10);
        tiersLimit.put("silver", 10);
        tiersLimit.put("bronze", 10);

        List<String> topicsAutoscaling = config.getList("redborder.indexing.autoscaling.topics",
                Arrays.asList("rb_flow_post", "rb_event_post"));

        for (String topic : topicsAutoscaling) {
            String realData = "";

            if (topic.contains("rb_flow"))
                realData = "rb_flow";
            else if (topic.contains("rb_monitor"))
                realData = "rb_monitor";
            else if (topic.contains("rb_event"))
                realData = "rb_event";
            else if (topic.contains("rb_state"))
                realData = "rb_state";
            else if (topic.contains("rb_social"))
                realData = "rb_social";

            currentPartitions.put(realData, config.getInt("redborder.kafka." + topic + ".partitions", 4));
        }
    }

    public void updateEvents(String dataSource, Object events) {
        if (events != null) {
            if (events instanceof Long) {
                eventsState.put(dataSource, (Long) events);
            } else if (events instanceof Integer) {
                eventsState.put(dataSource, ((Integer) events).longValue());
            }
        }
    }

    public Map<String, Map<String, Object>> updateStates() {
        log.info("Starting updateState autoscaling ...");
        log.info("The current state is: " + eventsState);
        for (Map.Entry<String, Long> entry : eventsState.entrySet()) {
            String dataSourceTier = entry.getKey();

            String[] dataSourceArray = dataSourceTier.split("-autoscaling-");
            String dataSource = dataSourceArray[0];
            String tier = dataSourceArray[1];
            String realData = null;

            for (String topic : currentPartitions.keySet()) {
                if (dataSource.contains(topic)) {
                    realData = topic;
                    break;
                }
            }

            if (realData != null) {
                Long events = (entry.getValue() / window_time) * currentPartitions.get(realData);
                Map<String, Object> currentDataSourceMetaData = dataSourceState.get(dataSource);
                Integer partitions;
                Integer replicas;

                log.info("Current dataSource: {} -> metadata: {}", dataSource, currentDataSourceMetaData);
                Integer limit = tiersLimit.get(tier) != null ? tiersLimit.get(tier) : 1;

                Integer desirePartitions = (int) Math.ceil(events.floatValue() / eventsPerTask);

                if (currentDataSourceMetaData == null) {

                    if (desirePartitions == 0) {
                        partitions = 1;
                    } else if (desirePartitions <= limit) {
                        partitions = desirePartitions;
                    } else {
                        partitions = limit;
                    }

                    replicas = 1;

                    log.info("First time [" + dataSource + "] desirePartitions[{}], currentPartitions[{}]",
                            desirePartitions, partitions);
                } else {
                    partitions = (Integer) currentDataSourceMetaData.get("partitions");
                    replicas = (Integer) currentDataSourceMetaData.get("replicas");

                    Long actualEvents = partitions * eventsPerTask;

                    log.info("Support events: " + actualEvents + " toDown: " + actualEvents * downPercent + " toUp: " + actualEvents * upPercent);
                    log.info("Current(events[{}] partitions[{}])", events, partitions);

                    if (actualEvents * upPercent <= events) {

                        if (desirePartitions < limit) {
                            if (desirePartitions == 0) {
                                partitions = 1;
                            } else {
                                partitions = desirePartitions;
                            }
                            log.info("Increasing dataSource[{}], in [{}] partitions", dataSource, partitions);
                        } else {
                            partitions = limit;
                            log.warn("DataSource {} limited!! Tier[" + tier + "], Limits[{}], Limit[" + limit + "], DesirePartitions[" + desirePartitions + "]",
                                    dataSource, tiersLimit);
                        }
                    } else if (actualEvents * downPercent >= events) {
                        if (desirePartitions == 0) {
                            partitions = 1;
                        } else if (partitions > desirePartitions) {
                            partitions = desirePartitions;
                        }
                        log.info("Decreasing dataSource[{}], in [{}] partitions", dataSource, partitions);
                    }
                }

                Map<String, Object> dataSourceMetaData = new HashMap<>();
                dataSourceMetaData.put("partitions", partitions);
                dataSourceMetaData.put("replicas", replicas);
                dataSourceMetaData.put("tier", tier);

                log.info("Current dataSource[{}], metadata[{}], events[" + events + "]", dataSource, dataSourceMetaData);
                dataSourceState.put(dataSource, dataSourceMetaData);
            }
        }
        log.info("Ending updateState autoscaling ...");

        return dataSourceState;
    }

    public void resetStats() {
        for (String key : eventsState.keySet())
            eventsState.put(key, 0L);
    }
}
