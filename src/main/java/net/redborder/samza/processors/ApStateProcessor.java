package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.constants.Constants;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;

import java.util.Map;

public class ApStateProcessor extends Processor<Map<String, Object>>{
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_APSTATE_OUTPUT_TOPIC);
    private Counter messagesCounter;

    public ApStateProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        this.messagesCounter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        Map<String, Object> messageEnrichmentStore = this.storeManager.enrich(message);
        messagesCounter.inc();
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, messageEnrichmentStore));
    }

    @Override
    public String getName() {
        return "ap-state";
    }
}
