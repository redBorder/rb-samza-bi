/*
 * Copyright (c) 2015 ENEO Tecnologia S.L.
 * This file is part of redBorder.
 * redBorder is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * redBorder is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with redBorder. If not, see <http://www.gnu.org/licenses/>.
 */

package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.enrichments.IEnrich;
import net.redborder.samza.store.StoreManager;
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

public abstract class Processor {
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

    public static Processor getProcessor(String streamName, Config config, TaskContext context, StoreManager storeManager) {
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
                Constructor constructor = foundClass.getConstructor(StoreManager.class, EnrichManager.class);
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

    public abstract void process(Map<String, Object> message, MessageCollector collector);

    public abstract String getName();
}
