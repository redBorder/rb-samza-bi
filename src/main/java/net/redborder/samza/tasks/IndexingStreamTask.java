package net.redborder.samza.tasks;

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

public class IndexingStreamTask implements StreamTask, InitableTask {
    private static final SystemStream monitorSystemStream = new SystemStream("druid_monitor", MONITOR_TOPIC);

    private static final Logger log = LoggerFactory.getLogger(EnrichmentStreamTask.class);
    private Counter counter;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        this.counter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String stream = envelope.getSystemStreamPartition().getSystemStream().getStream();
        Map<String, Object> message = (Map<String, Object>) envelope.getMessage();
        SystemStream systemStream = null;

        if (stream.equals(ENRICHMENT_FLOW_OUTPUT_TOPIC)) {
            systemStream = new SystemStream(getSystemStreamName(message, "flow"), getDatasource(message, FLOW_DATASOURCE));
        } else if (stream.equals(ENRICHMENT_EVENT_OUTPUT_TOPIC)) {
            systemStream = new SystemStream(getSystemStreamName(message, "event"), getDatasource(message, EVENT_DATASOURCE));
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

    private String getSystemStreamName(Map<String, Object> message, String topic) {
        Object tier = message.get(Dimension.TIER);
        String flowBeam = "druid_" + topic + "_bronze";

        if (tier != null) {
            String tierStr = String.valueOf(tier);

            switch (tierStr) {
                case "gold":
                    flowBeam = "druid_" + topic + "_gold";
                    break;
                case "silver":
                    flowBeam = "druid_" + topic + "_silver";
                    break;
            }
        }

        return flowBeam;
    }

    private String getDatasource(Map<String, Object> message, String defaultDatasource) {
        Object deploymentId = message.get(Dimension.DEPLOYMENT_ID);
        String datasource = defaultDatasource;

        if (deploymentId != null) {
            String deploymentIdStr = String.valueOf(deploymentId);
            datasource = defaultDatasource + "_" + deploymentIdStr;
        }

        return datasource;
    }
}