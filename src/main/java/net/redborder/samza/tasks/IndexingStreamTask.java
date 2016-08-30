package net.redborder.samza.tasks;

import com.google.common.collect.Maps;
import net.redborder.samza.indexing.autoscaling.AutoScalingManager;
import net.redborder.samza.indexing.autoscaling.DataSourceMetadata;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static net.redborder.samza.util.constants.Constants.*;
import static net.redborder.samza.util.constants.Dimension.NAMESPACE_UUID;

public class IndexingStreamTask implements StreamTask, InitableTask, WindowableTask {

    private static final Logger log = LoggerFactory.getLogger(IndexingStreamTask.class);
    private AutoScalingManager autoScalingManager;
    private Counter counter;
    private Boolean useNamespace = true;
    Map<String, Map<String, Object>> dataSourcesStates = Maps.newHashMap();

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        this.counter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
        this.autoScalingManager = new AutoScalingManager(config);
        this.useNamespace = config.getBoolean("net.redborder.indexing.useNamespace", true);
    }

    @Override
    public void window(MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        dataSourcesStates.putAll(autoScalingManager.updateStates());
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String stream = envelope.getSystemStreamPartition().getSystemStream().getStream();
        Object msg = envelope.getMessage();
        SystemStream systemStream = null;

        if (msg instanceof Map) {
            Map<String, Object> message = (Map<String, Object>) msg;

            if (stream.equals(ENRICHMENT_FLOW_OUTPUT_TOPIC)) {
                systemStream = new SystemStream("druid_flow", getDatasource(message, FLOW_DATASOURCE));
            } else if (stream.equals(ENRICHMENT_EVENT_OUTPUT_TOPIC)) {
                systemStream = new SystemStream("druid_event", getDatasource(message, EVENT_DATASOURCE));
            } else if (stream.equals(STATE_TOPIC) || stream.equals(ENRICHMENT_APSTATE_OUTPUT_TOPIC)) {
                systemStream = new SystemStream("druid_state", getDatasource(message, STATE_DATASOURCE));
            } else if (stream.equals(SOCIAL_TOPIC)) {
                systemStream = new SystemStream("druid_social", getDatasource(message, SOCIAL_DATASOURCE));
            } else if (stream.equals(HASHTAGS_TOPIC)) {
                systemStream = new SystemStream("druid_hashtag", getDatasource(message, HASHTAGS_DATASOURCE));
            } else if (stream.equals(MALWARE_TOPIC)) {
                systemStream = new SystemStream("druid_malware", getDatasource(message, MALWARE_DATASOURCE));
            } else if (stream.equals(MONITOR_TOPIC)) {
                systemStream = new SystemStream("druid_monitor", getDatasource(message, MONITOR_TOPIC));
            } else if (stream.equals(LOCATION_POST_TOPIC)) {
                systemStream = new SystemStream("druid_location", getDatasource(message, ENRICHMENT_LOC_OUTPUT_TOPIC));
            } else if (stream.equals(IOC_TOPIC)) {
                systemStream = new SystemStream("druid_ioc", getDatasource(message, IOC_DATASOURCE));
            } else if (stream.equals(CHANGES_TOPIC)) {
                systemStream = new SystemStream("druid_changes", getDatasource(message, CHANGES_DATASOURCE));
            } else if (stream.equals(IOT_TOPIC)) {
                systemStream = new SystemStream("druid_iot", getDatasource(message, IOT_DATASOURCE));
            } else {
                log.warn("Undefined input stream name: " + stream);
            }

            if (systemStream != null) {
                try {
                    collector.send(new OutgoingMessageEnvelope(systemStream, getPR(systemStream.getStream()), message));
                    counter.inc();
                } catch (Exception ex) {
                    log.error("Error sending to tranquility!! ", ex);
                }
            }

        } else {
            log.warn("This message is not a map class: " + msg);
        }
    }

    private String getDatasource(Map<String, Object> message, String defaultDatasource) {
        Object namespaceId = message.get(NAMESPACE_UUID);

        String datasource = defaultDatasource;

        if (useNamespace && namespaceId != null) {
            Object flowsCount = message.get("flows_count");
            Integer maxPartitions = (Integer) message.get("index_partitions");
            Integer replicas = (Integer)message.get("index_replicas");

            String namespaceIdStr = String.valueOf(namespaceId);
            datasource = defaultDatasource + "_" + namespaceIdStr;

            autoScalingManager.updateEvents(datasource, new DataSourceMetadata(maxPartitions, replicas, flowsCount));
        }

        return datasource;
    }


    private int [] getPR(String dataSource) {
        Map<String, Object> dsMetada = dataSourcesStates.get(dataSource);
        Integer partitions = 1;
        Integer replicas = 1;

        if (dsMetada != null) {
            partitions = dsMetada.get("partitions") == null ? 1 : (Integer) dsMetada.get("partitions");
        }

        if (dsMetada != null) {
            replicas = dsMetada.get("replicas") == null ? 1 : (Integer) dsMetada.get("replicas");
        }

        return new int[]{partitions, replicas};
    }
}