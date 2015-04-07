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
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsVisitor;
import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;
import static net.redborder.samza.util.constants.DimensionValue.NMSP_TYPE_INFO;
import static net.redborder.samza.util.constants.DimensionValue.NMSP_TYPE_MEASURE;

public class NmspProcessor extends Processor {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("druid", "rb_flow");
    public final static String NMSP_STORE_MEASURE = "nmsp-measure";
    public final static String NMSP_STORE_INFO = "nmsp-info";

    private KeyValueStore<String, Map<String, Object>> storeMeasure;
    private KeyValueStore<String, Map<String, Object>> storeInfo;
    private Counter messagesCounter;

    public NmspProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        this.messagesCounter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
        storeMeasure = storeManager.getStore(NMSP_STORE_MEASURE);
        storeInfo = storeManager.getStore(NMSP_STORE_INFO);
    }

    @Override
    public String getName() {
        return "nmsp";
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        Map<String, Object> toCache = new HashMap<>();
        Map<String, Object> toDruid = new HashMap<>();

        String type = (String) message.get(TYPE);
        String mac = (String) message.remove(CLIENT_MAC);

        if (type != null && type.equals(NMSP_TYPE_MEASURE)) {
            List<String> apMacs = (List<String>) message.get(NMSP_AP_MAC);
            List<Integer> clientRssis = (List<Integer>) message.get(NMSP_RSSI);
            message.remove(type);

            if (clientRssis != null && apMacs != null && !apMacs.isEmpty() && !clientRssis.isEmpty()) {
                Integer rssi = Collections.max(clientRssis);
                String apMac = apMacs.get(clientRssis.indexOf(rssi));

                if (rssi == 0)
                    toCache.put(CLIENT_RSSI, "unknown");
                else if (rssi <= -85)
                    toCache.put(CLIENT_RSSI, "bad");
                else if (rssi <= -80)
                    toCache.put(CLIENT_RSSI, "low");
                else if (rssi <= -70)
                    toCache.put(CLIENT_RSSI, "medium");
                else if (rssi <= -60)
                    toCache.put(CLIENT_RSSI, "good");
                else
                    toCache.put(CLIENT_RSSI, "excelent");

                Map<String, Object> infoCache = storeInfo.get(mac);
                String dot11Status;

                if (infoCache == null) {
                    toCache.put(CLIENT_RSSI_NUM, rssi);
                    toCache.put(WIRELESS_STATION, apMac);
                    toCache.put(NMSP_DOT11STATUS, "ASSOCIATED");
                    dot11Status = "PROBING";
                } else {
                    String apAssociated = (String) infoCache.get(WIRELESS_STATION);
                    if (apMacs.contains(apAssociated)) {
                        Integer rssiAssociated = clientRssis.get(apMacs.indexOf(apAssociated));
                        toCache.put(CLIENT_RSSI_NUM, rssiAssociated);
                        toCache.putAll(infoCache);
                        dot11Status = "ASSOCIATED";
                    } else {
                        toCache.put(CLIENT_RSSI_NUM, rssi);
                        toCache.put(WIRELESS_STATION, apMac);
                        toCache.put(NMSP_DOT11STATUS, "ASSOCIATED");
                        dot11Status = "PROBING";
                    }
                }

                toDruid.put(BYTES, 0);
                toDruid.put(PKTS, 0);
                toDruid.put(TYPE, "nmsp-measure");
                toDruid.put(CLIENT_MAC, mac);
                toDruid.putAll(toCache);
                toDruid.put(NMSP_DOT11STATUS, dot11Status);

                storeMeasure.put(mac, toCache);
                toDruid.put("timestamp", System.currentTimeMillis() / 1000);
                collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, toDruid));
            }
        } else if (type != null && type.equals(NMSP_TYPE_INFO)) {
            Object vlan = message.remove(NMSP_VLAN_ID);
            message.remove(type);

            if (vlan != null) {
                toCache.put(SRC_VLAN, vlan);
            }

            toCache.putAll(message);
            toDruid.putAll(toCache);
            toDruid.put(BYTES, 0);
            toDruid.put(PKTS, 0);
            toDruid.put(TYPE, "nmsp-info");
            toDruid.put(CLIENT_MAC, mac);
            storeInfo.put(mac, toCache);
            toDruid.put("timestamp", System.currentTimeMillis() / 1000);
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, toDruid));
        }

        this.messagesCounter.inc();
    }
}
