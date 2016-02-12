package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.constants.Constants;
import org.apache.samza.config.Config;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EventProcessor extends Processor<Map<String, Object>> {
    private static final Logger log = LoggerFactory.getLogger(EventProcessor.class);
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_EVENT_OUTPUT_TOPIC);


    public EventProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
    }

    @Override
    public String getName() {
        return "event";
    }

    @Override
    public void process(String stream, Map<String, Object> message, MessageCollector collector) {
        Map<String, Object> messageEnrichmentStore = this.storeManager.enrich(message);
        Map<String, Object> messageEnrichmentLocal = this.enrichManager.enrich(messageEnrichmentStore);

        log.trace(messageEnrichmentLocal.toString());
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, messageEnrichmentLocal));
    }
}
