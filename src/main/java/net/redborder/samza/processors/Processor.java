package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.enrichments.IEnrich;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.PostgresqlManager;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Processor<T> {
    private static final Logger log = LoggerFactory.getLogger(Processor.class);
    private static Map<String, Processor> processors = new HashMap<>();

    protected StoreManager storeManager;
    protected EnrichManager enrichManager;
    protected Config config;
    protected TaskContext context;

    public Processor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        this.storeManager = storeManager;
        this.enrichManager = enrichManager;
        this.config = config;
        this.context = context;
    }

    public static Processor getProcessor(String streamName, Config config, TaskContext context, StoreManager storeManager, PostgresqlManager postgresqlManager) {
        if (!processors.containsKey(streamName)) {
            List<String> enrichments;
            EnrichManager enrichManager = new EnrichManager();

            log.info("Asked for processor " + streamName + " but it wasn't found. Lets try to create it.");

            try {
                enrichments = config.getList("redborder.enrichments.streams." + streamName);
            } catch (ConfigException e) {
                log.info("Stream " + streamName + " does not have enrichments enabled");
                enrichments = new ArrayList<>();
            }

            for (String enrichment : enrichments) {
                try {
                    String className = config.get("redborder.enrichments.types." + enrichment);

                    if (className != null) {
                        Class enrichClass = Class.forName(className);
                        IEnrich enrich = (IEnrich) enrichClass.newInstance();
                        enrich.setPostgresqlManager(postgresqlManager);
                        enrichManager.addEnrichment(enrich);
                    } else {
                        log.warn("Couldn't find property redborder.enrichments.types." + enrichment + " on config properties");
                    }
                } catch (ClassNotFoundException e) {
                    log.error("Couldn't find the class associated with the enrichment " + enrichment);
                } catch (InstantiationException | IllegalAccessException e) {
                    log.error("Couldn't create the instance associated with the enrichment " + enrichment, e);
                }
            }

            try {
                String className = config.get("redborder.processors." + streamName);
                Class foundClass = Class.forName(className);
                Constructor constructor = foundClass.getConstructor(StoreManager.class, EnrichManager.class, Config.class, TaskContext.class);
                Processor processor = (Processor) constructor.newInstance(new Object [] { storeManager, enrichManager, config, context});
                processors.put(streamName, processor);
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the stream " + streamName);
                processors.put(streamName, new DummyProcessor());
            } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the stream " + streamName, e);
                processors.put(streamName, new DummyProcessor());
            }
        }

        return processors.get(streamName);
    }

    public abstract void process(T message, MessageCollector collector);

    public abstract String getName();
}
