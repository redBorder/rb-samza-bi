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

import net.redborder.samza.store.StoreManager;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public abstract class Processor {
    private static final Logger log = LoggerFactory.getLogger(Processor.class);
    private static Map<String, Processor> processors = new HashMap<>();

    public static Processor getProcessor(String streamName, Config config, StoreManager storeManager) {
        if (!processors.containsKey(streamName)) {
            log.info("Asked for processor " + streamName + " but it wasn't found. Lets try to create it.");
            String className = config.get("redborder.processors." + streamName + ".class");

            try {
                Class foundClass = Class.forName(className);
                Constructor constructor = foundClass.getConstructor(StoreManager.class);
                Processor processor = (Processor) constructor.newInstance(new Object [] { storeManager });
                processors.put(streamName, processor);
            } catch (ClassNotFoundException e) {
                log.error("Couldnt find the class associated with the stream " + streamName, e);
                processors.put(streamName, new DummyProcessor());
            } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
                log.error("Couldnt create the instance associated with the stream " + streamName, e);
                processors.put(streamName, new DummyProcessor());
            }
        }

        return processors.get(streamName);
    }

    public abstract Map<String, Object> process(Map<String, Object> message);
}