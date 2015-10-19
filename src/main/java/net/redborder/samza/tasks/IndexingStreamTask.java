package net.redborder.samza.tasks;

import net.redborder.samza.indexing.autoscaling.AutoScalingManager;
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
import static net.redborder.samza.util.constants.Dimension.*;

public class IndexingStreamTask implements StreamTask, InitableTask, WindowableTask {
    private static final SystemStream monitorSystemStream = new SystemStream("druid_monitor", MONITOR_TOPIC);

    private static final Logger log = LoggerFactory.getLogger(EnrichmentStreamTask.class);
    private AutoScalingManager autoScalingManager;
    private Counter counter;
    private Boolean useNamespace = true;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        this.counter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
        this.autoScalingManager = new AutoScalingManager(config);
        this.useNamespace = config.getBoolean("net.redborder.indexing.useNamespace", true);
    }

    @Override
    public void window(MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        autoScalingManager.updateStates();
        autoScalingManager.resetStats();
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
                systemStream = monitorSystemStream;
            } else {
                log.warn("Undefined input stream name: " + stream);
            }

            if (systemStream != null) {
                collector.send(new OutgoingMessageEnvelope(systemStream, null, message));
                counter.inc();
            }

        } else {
            log.warn("This message is not a map class: " + msg);
        }
    }

    private String getDatasource(Map<String, Object> message, String defaultDatasource) {
        Object namespaceId = message.get(NAMESPACE_UUID);
        Object tier = message.get(TIER);

        if (tier == null)
            tier = "bronze";

        String datasource = defaultDatasource + "_" + "none" + "_" + tier + "_1_1";

        if (useNamespace && namespaceId != null) {
            String namespaceIdStr = String.valueOf(namespaceId);
            datasource = defaultDatasource + "_" + namespaceIdStr + "_" + tier;
            autoScalingManager.incrementEvents(datasource);
            datasource = autoScalingManager.getDataSourcerWithPR(datasource);
        }

        return datasource;
    }
}