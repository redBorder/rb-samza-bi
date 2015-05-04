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

import net.redborder.samza.processors.Processor;
import net.redborder.samza.store.StoreManager;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EnrichmentStreamTask implements StreamTask, InitableTask {
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
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String stream = envelope.getSystemStreamPartition().getSystemStream().getStream();
        Object message = envelope.getMessage();

        Processor processor = Processor.getProcessor(stream, this.config, this.context, this.storeManager);
        processor.process(message, collector);
        counter.inc();
    }
}
