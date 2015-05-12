package net.redborder.samza.util;

import org.apache.commons.lang.StringUtils;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AutoScalingManager {
    private static final Logger log = LoggerFactory.getLogger(AutoScalingManager.class);

    private static Map<String, Long> eventsState = new ConcurrentHashMap<>();
    private static Map<String, String> dataSourceState = new ConcurrentHashMap<>();
    private static Map<String, Integer> tiersLimit = new ConcurrentHashMap<>();

    private static Double upPercent = 0.80;
    private static Double downPercent = 0.20;
    private static long eventsPerTask = 10000;
    private static Integer currentPartitions;


    public static void config(Config config, Integer partitions) {
        eventsPerTask = config.getLong("redborder.indexing.eventsPerTask", 5000);
        upPercent = config.getDouble("redborder.indexing.upPercent", 0.80);
        downPercent = config.getDouble("redborder.indexing.downPercent", 0.20);
        currentPartitions = partitions;
        tiersLimit.put("gold", 1000);
        tiersLimit.put("silver", 1000);
        tiersLimit.put("bronze", 1000);

    }

    public static void incrementEvents(String dataSource) {
        Long events = eventsState.get(dataSource) != null ? eventsState.get(dataSource) : 0L;
        events++;
        eventsState.put(dataSource, events);
    }

    public static String getDataSourcerWithPR(String dataSource) {
        return dataSourceState.get(dataSource) != null ? dataSourceState.get(dataSource) : dataSource + "_bronze_1_1";
    }

    public static void updateStates() {
        log.info("Starting updateState autoscaling ...");
        log.info("The current state is: " + eventsState);
        for (Map.Entry<String, Long> entry : eventsState.entrySet()) {
            String dataSource = entry.getKey();
            Long events = (entry.getValue() / 60) * currentPartitions;

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
                partitions = getPartitions(currentDataSource);
                replicas = getReplicas(currentDataSource);
                tier = getTier(currentDataSource);

                Long actualEvents = partitions * eventsPerTask;

                log.debug("Support events: " + actualEvents + " toDown: " + actualEvents * downPercent + " toUp: " + actualEvents * upPercent);
                log.debug("Current events: " + events);
                log.debug("Current partitions: " + partitions);

                if (actualEvents * upPercent <= events) {
                    if(tiersLimit.get(tier) < partitions) {
                        partitions++;
                    } else{
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
        log.info("Ending updateState autoscaling ...");
    }

    public static void resetStats() {
        for (String key : eventsState.keySet())
            eventsState.put(key, 0L);
    }

    public static Integer getPartitions(String dataSource) {
        String data [] = dataSource.split("_");
        return Integer.valueOf(data[data.length - 2]);
    }

    public static Integer getReplicas(String dataSource) {
        String data [] = dataSource.split("_");
        return Integer.valueOf(data[data.length - 1]);
    }

    public static String getTier(String dataSource){
        String data [] = dataSource.split("_");
        return data[data.length - 3];
    }

    public static String getDataSource(String dataSource){
        String data [] = dataSource.split("_");
        String subData [] = Arrays.copyOfRange(data, 0, data.length - 3);
        return StringUtils.join(subData, "_");
    }


}
