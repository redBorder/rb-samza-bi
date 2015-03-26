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

package net.redborder.samza.tasks;

import net.redborder.samza.processors.IProcessor;
import net.redborder.samza.store.StoreManager;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Map;

public class EnrichmentStreamTask implements StreamTask, InitableTask {
    private static final Logger log = LoggerFactory.getLogger(EnrichmentStreamTask.class);

    private final SystemStream OUTPUT_STREAM = new SystemStream("druid", "rb_flow");
    private Config config;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        StoreManager.init(config, context);
        this.config = config;
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String stream = envelope.getSystemStreamPartition().getSystemStream().getStream();
        Map<String, Object> message = (Map<String, Object>) envelope.getMessage();
        Map<String, Object> output;

        String className = this.config.get("redborder.processors." + stream + ".class");
        Class messageClass = Class.forName(className);
        Method method = messageClass.getMethod("getInstance");
        IProcessor processor = (IProcessor) method.invoke(null, null);
        output = processor.process(message);

        if (output != null) {
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, output));
        }
    }
}
