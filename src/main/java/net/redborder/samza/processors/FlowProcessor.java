package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.functions.CalculateDurationFunction;
import net.redborder.samza.functions.SplitFlowFunction;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.constants.Constants;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class FlowProcessor extends Processor<Map<String, Object>> {
    private static final Logger log = LoggerFactory.getLogger(FlowProcessor.class);
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_OUTPUT_TOPIC);

    private Counter messagesCounter;

    public FlowProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        this.messagesCounter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
    }

    @Override
    public String getName() {
        return "flow";
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        Map<String, Object> messageEnrichmentStore = this.storeManager.enrich(message);
        Map<String, Object> messageEnrichmentLocal = this.enrichManager.enrich(messageEnrichmentStore);

        messageEnrichmentLocal = CalculateDurationFunction.execute(messageEnrichmentLocal);
        List<Map<String, Object>> splittedMsg = SplitFlowFunction.split(messageEnrichmentLocal);

        for (Map<String, Object> msg : splittedMsg) {
            log.trace(messageEnrichmentLocal.toString());
            this.messagesCounter.inc();
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, msg));
        }
    }
}
