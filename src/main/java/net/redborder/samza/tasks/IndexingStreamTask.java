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
import static net.redborder.samza.util.constants.Constants.*;

import java.util.Map;

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

        if (stream.equals(ENRICHMENT_OUTPUT_TOPIC)) {
            String tenant_id = (String) message.get(Dimension.DEPLOYMENT_ID);

            if (tenant_id != null)
                systemStream = new SystemStream("druid", FLOW_DATASOURCE + "_" + tenant_id);
            else
                systemStream = new SystemStream("druid", FLOW_DATASOURCE);

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
