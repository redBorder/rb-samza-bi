package net.redborder.samza.tasks;

import net.redborder.samza.processors.Processor;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.PostgresqlManager;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EnrichmentStreamTask implements StreamTask, InitableTask, WindowableTask {
    private static final Logger log = LoggerFactory.getLogger(EnrichmentStreamTask.class);

    private Config config;
    private StoreManager storeManager;
    private PostgresqlManager postgresqlManager;
    private TaskContext context;
    private Counter counter;
    private KeyValueStore<String, Long> countersStore;
    private KeyValueStore<String, Long> flowsNumberStore;
    private final Integer FIVE_MINUTES = 5;
    private Integer windowTimes = 0;


    @Override
    public void init(Config config, TaskContext context) throws Exception {
        this.config = config;
        this.context = context;
        this.storeManager = new StoreManager(config, context);
        this.postgresqlManager = new PostgresqlManager(config, storeManager);
        this.counter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
        this.countersStore = (KeyValueStore<String, Long>) context.getStore("counter");
        this.flowsNumberStore = (KeyValueStore<String, Long>) context.getStore("flows-number");

        List<String> toDelete1 = new ArrayList<>();
        KeyValueIterator<String, Long> iter1 = flowsNumberStore.all();
        while(iter1.hasNext()){
            toDelete1.add(iter1.next().getKey());
        }

        flowsNumberStore.deleteAll(toDelete1);

        List<String> toDelete2 = new ArrayList<>();
        KeyValueIterator<String, Long> iter2 = countersStore.all();
        while(iter2.hasNext()){
            toDelete2.add(iter2.next().getKey());
        }

        countersStore.deleteAll(toDelete2);

    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String stream = envelope.getSystemStreamPartition().getSystemStream().getStream();
        Object message = envelope.getMessage();


        List<Processor> processors = Processor.getProcessors(stream, this.config, this.context, this.storeManager, this.postgresqlManager);
        if (message instanceof Map) {
            for(Processor processor : processors) {
                processor.process(stream, message, collector);
            }
            counter.inc();
        } else {
            log.warn("This message is not a map class: " + message);
        }
    }

    @Override
    public void window(MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        log.info("Window Calling [{}]", windowTimes);
        if(windowTimes.equals(FIVE_MINUTES)) {
            postgresqlManager.update();
            postgresqlManager.updateSalts();
            windowTimes = 0;
        }

        KeyValueIterator<String, Long> iter = countersStore.all();

        List<String> toReset = new ArrayList<>();
        while(iter.hasNext()){
            Entry<String, Long> count = iter.next();
            log.debug("Updating flows count [{}]  [{}]", count.getKey(), count.getValue());
            flowsNumberStore.put(count.getKey(), count.getValue());
            toReset.add(count.getKey());

            Map<String, Object> metric = makeMetric(count.getKey(), count.getValue());

            if(!metric.isEmpty()) {
                messageCollector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "rb_monitor"), metric));
            }
        }

        for(String key : toReset){
            countersStore.put(key, 0L);
        }

        windowTimes++;
    }

    private Map<String, Object> makeMetric(String key, Long value){
        Map<String, Object> metric = new HashMap<>();

        if(key != null && value != null) {
            String taskName = context.getTaskName().getTaskName().split(" ")[1];
            Integer container = context.getSamzaContainerContext().id;

            String[] keys = key.split("_");

            if(keys.length>=3){
                metric.put("namespace_uuid", keys[keys.length - 1]);
            }

            metric.put("type", "enrichmentstreamtask");
            metric.put("monitor", "enrichmentstreamtask_messages");
            metric.put("timestamp", System.currentTimeMillis() / 1000L);
            metric.put("sensor_name", String.format("samza-%s-%s-%s", container, taskName, key));
            metric.put("value", value);
        }

        return metric;
    }
}
