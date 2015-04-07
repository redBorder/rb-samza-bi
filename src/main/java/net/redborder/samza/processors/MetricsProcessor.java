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
import net.redborder.samza.store.StoreManager;
import org.apache.samza.config.Config;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MetricsProcessor extends Processor {
    private static final Logger log = LoggerFactory.getLogger(MetricsProcessor.class);
    private static final SystemStream OUTPUT_STREAM = new SystemStream("druid_monitor", "rb_monitor");

    public MetricsProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
    }

    @Override
    public String getName() {
        return "metrics";
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        Map<String, Object> asMap = (Map<String, Object>) message.get("asMap");
        Map<String, Object> metrics = (Map<String, Object>) asMap.get("metrics");
        Map<String, Object> header = (Map<String, Object>) asMap.get("header");

        for (Map.Entry<String, Object> classEntry : metrics.entrySet()) {
            Map<String, Object> contents = (Map<String, Object>) classEntry.getValue();

            for (Map.Entry<String, Object> contentEntry : contents.entrySet()) {
                Map<String, Object> toDruid = new HashMap<>();
                String className = classEntry.getKey();
                if(className.contains("net.redborder")) {
                    String[] classNameSeparated = className.split("\\.");

                    if (classNameSeparated.length != 0) {
                        className = classNameSeparated[classNameSeparated.length - 1];
                    }

                    Long timestamp = (Long) header.get("time");
                    timestamp = timestamp / 1000L;

                    toDruid.put("sensor_name", header.get("host") + ":" + header.get("container-name"));
                    toDruid.put("type", className);
                    toDruid.put("monitor", className + "_" + contentEntry.getKey());
                    toDruid.put("value", contentEntry.getValue());
                    toDruid.put("timestamp", timestamp);

                    collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, toDruid));
                }
            }
        }
    }
}
