package net.redborder.samza.indexing.autoscaling;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AutoScalingManager {
    private static final Logger log = LoggerFactory.getLogger(AutoScalingManager.class);
    private final Integer TIME_MIN = 30;

    private Map<String, Long> eventsState = new ConcurrentHashMap<>();
    private Map<String, String> dataSourceState = new ConcurrentHashMap<>();
    private Map<String, Integer> tiersLimit = new ConcurrentHashMap<>();
    private Map<String, Integer> currentPartitions = new HashMap<>();

    private static Double upPercent = 0.80;
    private static Double downPercent = 0.20;
    private static long eventsPerTask = 10000;

    public AutoScalingManager(Config config) {
        eventsPerTask = config.getLong("redborder.indexing.eventsPerTask", 5000);
        upPercent = config.getDouble("redborder.indexing.upPercent", 0.80);
        downPercent = config.getDouble("redborder.indexing.downPercent", 0.20);
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

    public void incrementEvents(String dataSource) {
        Long events = eventsState.get(dataSource) != null ? eventsState.get(dataSource) : 0L;
        events++;
        eventsState.put(dataSource, events);
    }

    public String getDataSourcerWithPR(String dataSource) {
        return dataSourceState.get(dataSource) != null ? dataSourceState.get(dataSource) : dataSource + "_bronze_1_1";
    }

    public void updateStates() {
        log.info("Starting updateState autoscaling ...");
        log.info("The current state is: " + eventsState);
        for (Map.Entry<String, Long> entry : eventsState.entrySet()) {
            String dataSource = entry.getKey();

            String realData = null;

            log.info("Current partitions {}", currentPartitions);
            for (String topic : currentPartitions.keySet()) {
                if (dataSource.contains(topic)) {
                    realData = topic;
                    break;
                }
            }

            if (realData != null) {
                Long events = (entry.getValue() / TIME_MIN) * currentPartitions.get(realData);
                String currentDataSource = dataSourceState.get(dataSource);
                Integer partitions;
                Integer replicas;
                String tier;
                log.info("Current dataSource: " + currentDataSource);

                if (currentDataSource == null) {
                    Integer round = Math.round(events / eventsPerTask);
                    partitions = round == 0 ? 1 : round;
                    replicas = 1;
                    tier = "bronze";
                } else {
                    partitions = AutoScalingUtils.getPartitions(currentDataSource);
                    replicas = AutoScalingUtils.getReplicas(currentDataSource);
                    tier = AutoScalingUtils.getTier(currentDataSource);

                    Long actualEvents = partitions * eventsPerTask;

                    log.info("Support events: " + actualEvents + " toDown: " + actualEvents * downPercent + " toUp: " + actualEvents * upPercent);
                    log.info("Current events: " + events);
                    log.info("Current partitions: " + partitions);

                    if (actualEvents * upPercent <= events) {
                        Integer limit = tiersLimit.get(tier) != null ? tiersLimit.get(tier) : 1;
                        if (partitions < limit) {
                            partitions++;
                        } else {
                            log.warn("This dataSource is " + tier + " and it has " + partitions + " partitions! LIMITED.");
                        }
                    } else if (actualEvents * downPercent >= events) {
                        if (partitions > 1) {
                            partitions--;
                        } else {
                            partitions = 1;
                        }
                    }

                    log.info("New partitions: " + partitions);
                }

                String updateDataSource = dataSource + "_" + tier + "_" + partitions + "_" + replicas;
                log.info("Datasource: " + dataSource + " -> " + updateDataSource);
                dataSourceState.put(dataSource, updateDataSource);
            }
        }
        log.info("Ending updateState autoscaling ...");
    }

    public void resetStats() {
        for (String key : eventsState.keySet())
            eventsState.put(key, 0L);
    }
}
