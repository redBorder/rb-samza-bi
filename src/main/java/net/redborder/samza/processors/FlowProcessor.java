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

import net.redborder.samza.util.constants.Dimension;
import org.apache.samza.storage.kv.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FlowProcessor implements IProcessor {
    private static final Logger log = LoggerFactory.getLogger(FlowProcessor.class);
    private static FlowProcessor instance = null;

    final private static String NMSP_STORE = "rb_flow";
    private KeyValueStore<String, Object> nmspStore;

    private FlowProcessor() {
        this.nmspStore = StoreManager.getStore(NMSP_STORE);
    }

    public FlowProcessor getInstance() {
        if (instance == null) instance = new FlowProcessor();
        return instance;
    }

    @Override
    public Map<String, Object> process(Map<String, Object> message) {
        Map<String, Object> output = new HashMap<>();
        String mac = (String) message.get(Dimension.CLIENT_MAC);
        Map<String, Object> nmspData = (Map<String, Object>) this.nmspStore.get(mac);

        output.putAll(message);

        if (nmspData != null && !nmspData.isEmpty()) {
            log.info("Cache hit at mac " + mac + " with data " + nmspData);
            output.putAll(nmspData);
        }

        return output;
    }
}
