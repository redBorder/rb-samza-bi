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
import net.redborder.samza.util.constants.Dimension;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class FlowProcessor extends Processor {
    private static final Logger log = LoggerFactory.getLogger(FlowProcessor.class);
    private static final SystemStream OUTPUT_STREAM = new SystemStream("druid", "rb_flow");
    private StoreManager storeManager;
    private EnrichManager enrichManager;

    public FlowProcessor(StoreManager storeManager, EnrichManager enrichManager) {
        this.storeManager = storeManager;
        this.enrichManager = enrichManager;
    }

    @Override
    public String getName() {
        return "flow";
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        String mac = (String) message.get(Dimension.CLIENT_MAC);
        String ip = (String) message.get(Dimension.SRC_IP);
        Map<String, Object> enrichData = this.storeManager.enrich(mac, ip);

        if (enrichData != null && !enrichData.isEmpty()) {
            message.putAll(enrichData);
        }



        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, message));
    }
}
