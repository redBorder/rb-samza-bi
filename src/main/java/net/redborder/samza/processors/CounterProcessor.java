package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.constants.Dimension;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;

import java.util.HashMap;
import java.util.Map;

public class CounterProcessor extends Processor<Map<String, Object>> {
    Map<String, Counter> counters;

    public CounterProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        this.context = context;
        this.counters = new HashMap<>();
    }

    @Override
    public void process(String stream, Map<String, Object> message, MessageCollector collector) {
        String namespaceUUID = (String) message.get(Dimension.NAMESPACE_UUID);

        String baseStream = "";

        if (stream.contains("rb_flow"))
            baseStream = "rb_flow";
        else if (stream.contains("rb_monitor"))
            baseStream = "rb_monitor";
        else if (stream.contains("rb_event"))
            baseStream = "rb_event";
        else if (stream.contains("rb_state"))
            baseStream = "rb_state";
        else if (stream.contains("rb_social"))
            baseStream = "rb_social";

        String id = namespaceUUID != null ? baseStream + "_" + namespaceUUID : baseStream;

        Counter count = counters.get(id);

        if (count == null) {
            count = context.getMetricsRegistry().newCounter("flowsec_per_namespace", new Counter(id));
        }

        count.inc();
        counters.put(id, count);
    }

    @Override
    public String getName() {
        return "counter";
    }
}
