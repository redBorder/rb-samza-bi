package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.functions.CalculateDurationFunction;
import net.redborder.samza.functions.SplitFlowFunction;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.constants.Constants;
import net.redborder.samza.util.constants.Dimension;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
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
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_FLOW_OUTPUT_TOPIC);
    private static final String DATASOURCE = "rb_flow";

    private KeyValueStore<String, Long> countersStore;
    private KeyValueStore<String, Long> flowsNumber;

    public FlowProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        countersStore = (KeyValueStore<String, Long>) context.getStore("counter");
        flowsNumber = (KeyValueStore<String, Long>) context.getStore("flows-number");
    }

    @Override
    public String getName() {
        return "flow";
    }

    @Override
    public void process(String stream, Map<String, Object> message, MessageCollector collector) {
        Map<String, Object> messageEnrichmentStore = this.storeManager.enrich(message);
        Map<String, Object> messageEnrichmentLocal = this.enrichManager.enrich(messageEnrichmentStore);

        messageEnrichmentLocal = CalculateDurationFunction.execute(messageEnrichmentLocal);

        if(!messageEnrichmentLocal.containsKey(Dimension.WIRELESS_STATION_NAME)){
            if(messageEnrichmentLocal.containsKey(Dimension.WIRELESS_STATION)) {
                messageEnrichmentLocal.put(Dimension.WIRELESS_STATION_NAME, "unknown/unknown/" + messageEnrichmentLocal.get(Dimension.WIRELESS_STATION));
            }
        }

        List<Map<String, Object>> splittedMsg = SplitFlowFunction.split(messageEnrichmentLocal);

        String datasource = DATASOURCE;
        Object namespace = messageEnrichmentLocal.get(Dimension.NAMESPACE_UUID);

        if (namespace != null) {
            datasource = String.format("%s_%s", DATASOURCE, namespace.toString());
        }

        Long counter = countersStore.get(datasource);

        if(counter == null){
            counter = 0L;
        }

        Long flows = flowsNumber.get(datasource);

        for (Map<String, Object> msg : splittedMsg) {
            counter++;
            if(flows != null) {
                msg.put("flows_count", flows);
            }
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, msg));
        }

        countersStore.put(datasource, counter);
    }
}
