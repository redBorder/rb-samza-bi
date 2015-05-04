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
