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
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EnrichmentStreamTask implements StreamTask, InitableTask {
    private static final Logger log = LoggerFactory.getLogger(EnrichmentStreamTask.class);

    private KeyValueStore<String, Map<String, Object>> store;
    private final SystemStream OUTPUT_STREAM = new SystemStream("druid", "rb_flow");
    private Config config;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        this.store = (KeyValueStore<String, Map<String, Object>>) context.getStore("nmsp");
        this.config = config;
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Map<String, Object> message = (Map<String, Object>) envelope.getMessage();
        String mac = (String) message.get("client_mac");
        Map<String, Object> output;
        Map<String, Object> cached;
        Map<String, Object> toCache;
        List<String> wireless_stations;
        String type, wireless_station;
        String stream = envelope.getSystemStreamPartition().getSystemStream().getStream();

        if (mac != null) {
            if (stream.equals("rb_nmsp")) {
                toCache = new HashMap<>();

                type = (String) message.get("type");
                if (type != null && type.equals("measure")) {
                    wireless_stations = (List<String>) message.get("ap_mac");

                    if (wireless_stations != null && !wireless_stations.isEmpty()) {
                        wireless_station = wireless_stations.get(0);
                        toCache.put("wireless_station", wireless_station);
                    }
                }

                this.store.put(mac, toCache);
            } else {
                output = new HashMap<>();
                cached = this.store.get(mac);

                output.putAll(message);

                if (cached != null && !cached.isEmpty()) {
                    log.info("Cache hit at mac " + mac + " with data " + cached);
                    output.putAll(cached);
                }

                collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, output));
            }
        }
    }
}
