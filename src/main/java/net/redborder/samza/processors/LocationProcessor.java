package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import org.apache.samza.config.Config;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.LOC_NOTIFICATIONS;
import static net.redborder.samza.util.constants.Dimension.LOC_STREAMING_NOTIFICATION;

public class LocationProcessor extends Processor<Map<String, Object>> {
    private static final Logger log = LoggerFactory.getLogger(LocationProcessor.class);
    private LocationV89Processor locv89;
    private LocationV10Processor locv10;

    public LocationProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        locv89 = new LocationV89Processor(storeManager, enrichManager, config, context);
        locv10 = new LocationV10Processor(storeManager, enrichManager, config, context);
    }

    @Override
    public String getName() {
        return "loc";
    }

    @Override
    @SuppressWarnings("unchecked cast")
    public void process(String stream, Map<String, Object> message, MessageCollector collector) {
        if (message.containsKey(LOC_STREAMING_NOTIFICATION)) {
            locv89.process(message, collector);
        } else if (message.containsKey(LOC_NOTIFICATIONS)){
            locv10.process(message, collector);
        } else {
            log.warn("Unknow location message: {}", message);
        }
    }
}
