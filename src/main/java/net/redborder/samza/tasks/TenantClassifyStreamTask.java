package net.redborder.samza.tasks;

import net.redborder.samza.util.constants.Dimension;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Date: 13/4/15 16:50.
 */
public class TenantClassifyStreamTask  implements StreamTask, InitableTask {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentStreamTask.class);
    public final static String TENANT_STORE = "tenant";

    private KeyValueStore<String, SystemStream> store;
    private Counter counter;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        this.store = (KeyValueStore<String, SystemStream>) context.getStore(TENANT_STORE);
        initStore();
        this.counter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Map<String, Object> message = (Map<String, Object>) envelope.getMessage();
        String sensor_ip = (String) message.get(Dimension.SENSOR_IP);
        SystemStream outputStream = store.get(sensor_ip);
        collector.send(new OutgoingMessageEnvelope(outputStream, null, message));
        counter.inc();
    }

    public void initStore(){

    }
}
