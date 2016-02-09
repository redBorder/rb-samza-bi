package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.constants.Constants;
import net.redborder.samza.util.constants.Dimension;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class ApStateProcessor extends Processor<Map<String, Object>> {
    private static final Logger log = LoggerFactory.getLogger(ApStateProcessor.class);
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_APSTATE_OUTPUT_TOPIC);
    private static final String DATASOURCE = "rb_state";
    private KeyValueStore<String, Long> countersStore;
    private KeyValueStore<String, Long> flowsNumber;

    public ApStateProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        countersStore = (KeyValueStore<String, Long>) context.getStore("counter");
        flowsNumber = (KeyValueStore<String, Long>) context.getStore("flows-number");
    }

    @Override
    public void process(String stream, Map<String, Object> message, MessageCollector collector) {
        Map<String, Object> messageEnrichmentStore = this.storeManager.enrich(message);

        String datasource = DATASOURCE;
        Object namespace = messageEnrichmentStore.get(Dimension.NAMESPACE_UUID);

        if (namespace != null) {
            datasource = String.format("%s_%s", DATASOURCE, namespace.toString());
        }

        Long counter = countersStore.get(datasource);

        if(counter == null){
            counter = 0L;
        }

        counter++;
        countersStore.put(datasource, counter);
        Long flows = flowsNumber.get(datasource);

        if(flows != null){
            messageEnrichmentStore.put("flows_count", flows);
        }

        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, messageEnrichmentStore));
    }

    @Override
    public String getName() {
        return "ap-state";
    }
}
