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

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class NmspEnrichStreamTask implements StreamTask, InitableTask {
    private static final Logger log = LoggerFactory.getLogger(NmspEnrichStreamTask.class);

    //private KeyValueStore<String, Map<String, Object>> store;
    private final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "rb_flow_with_nmsp");

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        //this.store = (KeyValueStore<String, Map<String, Object>>) context.getStore("nmsp");
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Map<String, Object> message = (Map<String, Object>) envelope.getMessage();
        String mac = (String) message.get("client_mac");
        log.info("Client mac is " + mac);

        //if (envelope.getSystemStreamPartition().getSystemStream().getStream().equals("rb_nmsp")) {
           // this.store.put(mac, message);
        //} else {
            //Map<String, Object> output = new HashMap<>();
            //output.putAll(message);
            //output.putAll(this.store.get(mac));

            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, message));
        //}
    }
}
