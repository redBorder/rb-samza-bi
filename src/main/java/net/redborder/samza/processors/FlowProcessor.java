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
import net.redborder.samza.functions.SplitFlowFunction;
import net.redborder.samza.store.StoreManager;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class FlowProcessor extends Processor {
    private static final Logger log = LoggerFactory.getLogger(FlowProcessor.class);
    private static final SystemStream OUTPUT_STREAM = new SystemStream("druid", "rb_flow");

    private Counter messagesCounter;

    public FlowProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        this.messagesCounter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
    }

    @Override
    public String getName() {
        return "flow";
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        Map<String, Object> messageEnrichmentStore = this.storeManager.enrich(message);
        Map<String, Object> messageEnrichmentLocal = this.enrichManager.enrich(messageEnrichmentStore);
        List<Map<String, Object>> splittedMsg = SplitFlowFunction.split(messageEnrichmentLocal);

        for (Map<String, Object> msg : splittedMsg) {
            log.trace(messageEnrichmentLocal.toString());
            this.messagesCounter.inc();
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, msg));
        }
    }
}
