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
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;

public class LocationV10Processor extends Processor {
    private static final Logger log = LoggerFactory.getLogger(LocationV10Processor.class);
    private static final SystemStream OUTPUT_STREAM = new SystemStream("druid", "rb_flow");
    final public static String LOCATION_STORE = "location";

    private KeyValueStore<String, Map<String, Object>> store;
    private Map<Integer, String> cache;

    public LocationV10Processor(StoreManager storeManager, EnrichManager enrichManager) {
        super(storeManager, enrichManager);

        store = storeManager.getStore(LOCATION_STORE);

        cache = new HashMap<>();
        cache.put(0, "IDLE");
        cache.put(1, "AAA_PENDING");
        cache.put(2, "AUTHENTICATED");
        cache.put(3, "ASSOCIATED");
        cache.put(4, "POWERSAVE");
        cache.put(5, "DISASSOCIATED");
        cache.put(6, "TO_BE_DELETED");
        cache.put(7, "PROBING");
        cache.put(8, "BLACK_LISTED");
        cache.put(256, "WAIT_AUTHENTICATED");
        cache.put(257, "WAIT_ASSOCIATED");
    }

    @Override
    public String getName() {
        return "locv10";
    }

    @Override
    @SuppressWarnings("unchecked cast")
    public void process(Map<String, Object> message, MessageCollector collector) {
        List<Map<String, Object>> notifications = (List<Map<String, Object>>) message.get(LOC_NOTIFICATIONS);

        for (Map<String, Object> notification : notifications) {
            String notificationType = (String) notification.get(LOC_NOTIFICATION_TYPE);
            if (notificationType == null) notificationType = "null";

            if (notificationType.equals("association")) {
                log.trace("Mse10 event this event is a association, emitting " + notification.size());
                processAssociation(message, collector);
            } else if (notificationType.equals("locationupdate")) {
                log.trace("Mse10 event this event is a locationupdate, emitting " + notification.size());
                processLocationUpdate(message, collector);
            } else {
               log.warn("MSE version 10 notificationType is unknown: " + notificationType);
            }
        }
    }

    @SuppressWarnings("unchecked cast")
    public void processAssociation(Map<String, Object> message, MessageCollector collector) {
        try {
            log.trace("Processing mse10Association " + message);
            Map<String, Object> toCache = new HashMap<>();
            Map<String, Object> toDruid = new HashMap<>();
            String clientMac = (String) message.get(LOC_DEVICEID);

            if (message.get(LOC_SSID) != null)
                toCache.put(WIRELESS_ID, message.get(LOC_SSID));

            if (message.get(LOC_BAND) != null)
                toCache.put(NMSP_DOT11PROTOCOL, message.get(LOC_BAND));

            if (message.get(LOC_STATUS) != null) {
                Integer msgStatus = (Integer) message.get(LOC_STATUS);
                toCache.put(DOT11STATUS, cache.get(msgStatus));
            }

            if (message.get(LOC_AP_MACADDR) != null)
                toCache.put(WIRELESS_STATION, message.get(LOC_AP_MACADDR));

            if (!message.get(LOC_USERNAME).equals(""))
                toCache.put(CLIENT_ID, message.get(LOC_USERNAME));

            toDruid.putAll(toCache);
            toDruid.put(SENSOR_NAME, message.get(LOC_SUBSCRIPTION_NAME));
            toDruid.put(CLIENT_MAC, clientMac);
            toDruid.put(TIMESTAMP, ((Long) message.get(TIMESTAMP)) / 1000L);
            toDruid.put(BYTES, 0);
            toDruid.put(PKTS, 0);
            toDruid.put(TYPE, "mse10");

            store.put(clientMac, toCache);
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, toDruid));
        } catch (Exception ex) {
            log.warn("MSE10 association event dropped: " + message, ex);
        }
    }

    @SuppressWarnings("unchecked cast")
    public void processLocationUpdate(Map<String, Object> message, MessageCollector collector) {
        try {
            log.trace("Processing mse10LocationUpdate " + message);

            Map<String, Object> toCache = new HashMap<>();
            Map<String, Object> toDruid = new HashMap<>();

            String clientMac = (String) message.get(LOC_DEVICEID);
            String locationMapHierarchy = (String) message.get(LOC_MAP_HIERARCHY_V10);

            if (locationMapHierarchy != null) {
                String[] locations = locationMapHierarchy.split(">");

                if (locations.length >= 1)
                    toCache.put(CLIENT_CAMPUS, locations[0]);
                if (locations.length >= 2)
                    toCache.put(CLIENT_BUILDING, locations[1]);
                if (locations.length >= 3)
                    toCache.put(CLIENT_FLOOR, locations[2]);
                if (locations.length >= 4)
                    toCache.put(CLIENT_ZONE, locations[3]);
            }

            toDruid.putAll(toCache);
            toDruid.put(SENSOR_NAME, message.get(LOC_SUBSCRIPTION_NAME));

            if (message.containsKey(TIMESTAMP)) {
                toDruid.put(TIMESTAMP, ((Long) message.get(TIMESTAMP)) / 1000L);
            } else {
                toDruid.put(TIMESTAMP, System.currentTimeMillis() / 1000L);
            }

            toDruid.put(BYTES, 0);
            toDruid.put(PKTS, 0);
            toDruid.put(TYPE, "mse10");

            store.put(clientMac, toCache);
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, toDruid));
        } catch (Exception ex) {
            log.warn("MSE10 locationUpdate event dropped: " + message, ex);
        }
    }
}
