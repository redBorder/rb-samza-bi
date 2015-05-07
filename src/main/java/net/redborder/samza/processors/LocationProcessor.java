package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;

import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.LOC_STREAMING_NOTIFICATION;

public class LocationProcessor extends Processor<Map<String, Object>> {
    private LocationV89Processor locv89;
    private LocationV10Processor locv10;
    private Counter counter;

    public LocationProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        locv89 = new LocationV89Processor(storeManager, enrichManager, config, context);
        locv10 = new LocationV10Processor(storeManager, enrichManager, config, context);
        counter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
    }

    @Override
    public String getName() {
        return "loc";
    }

    @Override
    @SuppressWarnings("unchecked cast")
    public void process(Map<String, Object> message, MessageCollector collector) {
        if (message.containsKey(LOC_STREAMING_NOTIFICATION)) {
            locv89.process(message, collector);
        } else {
            locv10.process(message, collector);
        }
        counter.inc();
    }
}
