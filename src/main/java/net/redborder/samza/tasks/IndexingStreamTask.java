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

import java.util.HashMap;
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

        if (stream.equals(ENRICHMENT_OUTPUT_TOPIC)) {
            Object deploymentId = message.get(Dimension.DEPLOYMENT_ID);

            if (deploymentId != null) {
                String deploymentIdStr = String.valueOf(deploymentId);
                String dataSource = FLOW_DATASOURCE + "_" + deploymentIdStr;
                AutoScalingManager.incrementEvents(dataSource);
                systemStream = new SystemStream("druid_flow", AutoScalingManager.getDataSourcerWithPR(dataSource));
            } else {
                systemStream = new SystemStream("druid_flow", FLOW_DATASOURCE + "1_1");
            }
        } else if (stream.equals(MONITOR_TOPIC)) {
            systemStream = monitorSystemStream;
        }

        if (systemStream != null) {
            collector.send(new OutgoingMessageEnvelope(systemStream, null, message));
            counter.inc();
        } else {
            log.warn("Not defined input stream name: " + stream);
        }
    }

}