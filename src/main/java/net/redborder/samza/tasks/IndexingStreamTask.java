package net.redborder.samza.tasks;

import net.redborder.samza.util.AutoScalingManager;
import net.redborder.samza.util.constants.Dimension;
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

public class IndexingStreamTask implements StreamTask, InitableTask, WindowableTask {
    private static final SystemStream monitorSystemStream = new SystemStream("druid_monitor", MONITOR_TOPIC);
    private static final Logger log = LoggerFactory.getLogger(EnrichmentStreamTask.class);
    private Counter counter;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        this.counter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
        AutoScalingManager.config(config, context.getSystemStreamPartitions().size());

    }

    @Override
    public void window(MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        AutoScalingManager.updateStates();
        AutoScalingManager.resetStats();
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String stream = envelope.getSystemStreamPartition().getSystemStream().getStream();
        Map<String, Object> message = (Map<String, Object>) envelope.getMessage();
        SystemStream systemStream = null;

        if (stream.equals(ENRICHMENT_FLOW_OUTPUT_TOPIC)) {
            systemStream = new SystemStream("druid_flow", getDatasource(message, FLOW_DATASOURCE));
        } else if (stream.equals(ENRICHMENT_EVENT_OUTPUT_TOPIC)) {
            systemStream = new SystemStream("druid_flow", getDatasource(message, EVENT_DATASOURCE));
        } else if (stream.equals(MONITOR_TOPIC)) {
            systemStream = monitorSystemStream;
        } else {
            log.warn("Undefined input stream name: " + stream);
        }

        if (systemStream != null) {
            collector.send(new OutgoingMessageEnvelope(systemStream, null, message));
            counter.inc();
        }
    }

    private String getDatasource(Map<String, Object> message, String defaultDatasource) {
        Object deploymentId = message.get(Dimension.DEPLOYMENT_ID);
        String datasource = defaultDatasource;

        if (deploymentId != null) {
            String deploymentIdStr = String.valueOf(deploymentId);
            datasource = defaultDatasource + "_" + deploymentIdStr;
            AutoScalingManager.incrementEvents(datasource);
            datasource = AutoScalingManager.getDataSourcerWithPR(datasource);
        }

        return datasource;
    }
}