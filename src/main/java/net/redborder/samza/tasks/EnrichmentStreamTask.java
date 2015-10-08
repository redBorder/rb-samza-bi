package net.redborder.samza.tasks;

import net.redborder.samza.processors.Processor;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.PostgresqlManager;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EnrichmentStreamTask implements StreamTask, InitableTask, WindowableTask {
    private static final Logger log = LoggerFactory.getLogger(EnrichmentStreamTask.class);

    private Config config;
    private StoreManager storeManager;
    private TaskContext context;
    private Counter counter;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        this.config = config;
        this.context = context;
        this.storeManager = new StoreManager(config, context);
        this.counter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
        PostgresqlManager.init(config, storeManager);
        PostgresqlManager.update();
        PostgresqlManager.updateSalts();
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String stream = envelope.getSystemStreamPartition().getSystemStream().getStream();
        Object message = envelope.getMessage();


        Processor processor = Processor.getProcessor(stream, this.config, this.context, this.storeManager);
        if (message instanceof Map) {
            processor.process(message, collector);
            counter.inc();
        } else {
            log.warn("This message is not a map class: " + message);
        }
    }

    @Override
    public void window(MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        PostgresqlManager.update();
        PostgresqlManager.updateSalts();
    }
}
