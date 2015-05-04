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
import net.redborder.samza.util.constants.Constants;
import net.redborder.samza.util.constants.Dimension;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;
import static net.redborder.samza.util.constants.DimensionValue.LOC_ASSOCIATED;

public class LocationV89Processor extends Processor<Map<String, Object>> {
    final public static String LOCATION_STORE = "location";
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_OUTPUT_TOPIC);

    private KeyValueStore<String, Map<String, Object>> store;

    private Counter counter;

    public LocationV89Processor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        store = storeManager.getStore(LOCATION_STORE);
        counter = context.getMetricsRegistry().newCounter(getClass().getName(), "messages");
    }

    @Override
    public String getName() {
        return "locv89";
    }

    @Override
    @SuppressWarnings("unchecked cast")
    public void process(Map<String, Object> message, MessageCollector collector) {
        Map<String, Object> mseEventContent, location, mapInfo, toCache, toDruid;
        Map<String, Object> geoCoordinate = null;
        String mapHierarchy, locationFormat, state;
        String macAddress = null;
        Double latitude, longitude;
        String[] zone;

        String deployment_id = message.get(Dimension.DEPLOYMENT_ID) == null ? "" : (String) message.get(Dimension.DEPLOYMENT_ID);
        mseEventContent = (Map<String, Object>) message.get(LOC_STREAMING_NOTIFICATION);

        if (mseEventContent != null) {
            location = (Map<String, Object>) mseEventContent.get(LOC_LOCATION);
            toCache = new HashMap<>();
            toDruid = new HashMap<>();

            if (location != null) {
                geoCoordinate = (Map<String, Object>) location.get(LOC_GEOCOORDINATEv8);
                if (geoCoordinate == null) {
                    geoCoordinate = (Map<String, Object>) location.get(LOC_GEOCOORDINATEv9);
                }

                mapInfo = (Map<String, Object>) location.get(LOC_MAPINFOv8);
                if (mapInfo == null) {
                    mapInfo = (Map<String, Object>) location.get(LOC_MAPINFOv9);
                }

                macAddress = (String) location.get(LOC_MACADDR);
                toDruid.put(CLIENT_MAC, macAddress);

                mapHierarchy = (String) mapInfo.get(LOC_MAP_HIERARCHY);

                if (mapHierarchy != null) {
                    zone = mapHierarchy.split(">");

                    if (zone.length >= 1)
                        toCache.put(CLIENT_CAMPUS, zone[0]);
                    if (zone.length >= 2)
                        toCache.put(CLIENT_BUILDING, zone[1]);
                    if (zone.length >= 3)
                        toCache.put(CLIENT_FLOOR, zone[2]);
                }

                state = (String) location.get(LOC_DOT11STATUS);

                if (state != null) {
                    toDruid.put(DOT11STATUS, state);
                    toCache.put(DOT11STATUS, state);
                }

                if (state != null && state.equals(LOC_ASSOCIATED)) {
                    List<String> ip = (List<String>) location.get(LOC_IPADDR);
                    if (location.get(LOC_SSID) != null)
                        toCache.put(WIRELESS_ID, location.get(LOC_SSID));
                    if (location.get(LOC_AP_MACADDR) != null)
                        toCache.put(WIRELESS_STATION, location.get(LOC_AP_MACADDR));
                    if (ip != null && ip.get(0) != null) {
                        toDruid.put(SRC, ip.get(0));
                    }
                }
            }

            if (geoCoordinate != null) {
                latitude = (Double) geoCoordinate.get(LOC_LATITUDEv8);
                if (latitude == null) {
                    latitude = (Double) geoCoordinate.get(LOC_LATITUDEv9);
                }

                latitude = (double) Math.round(latitude * 100000) / 100000;

                longitude = (Double) geoCoordinate.get(LOC_LONGITUDE);
                longitude = (double) Math.round(longitude * 100000) / 100000;

                locationFormat = latitude.toString() + "," + longitude.toString();

                toCache.put(CLIENT_LATLNG, locationFormat);
            }

            String dateString = (String) mseEventContent.get(TIMESTAMP);
            String sensorName = (String) mseEventContent.get(LOC_SUBSCRIPTION_NAME);

            if (sensorName != null) {
                toDruid.put(SENSOR_NAME, sensorName);
            }

            toDruid.putAll(toCache);
            toDruid.put(CLIENT_RSSI, "unknown");
            toDruid.put(CLIENT_RSSI_NUM, 0);
            toDruid.put(CLIENT_SNR, "unknown");
            toDruid.put(CLIENT_SNR_NUM, 0);

            if (!deployment_id.equals(""))
                toDruid.put(DEPLOYMENT_ID, deployment_id);

            toDruid.put(BYTES, 0);
            toDruid.put(PKTS, 0);
            toDruid.put(TYPE, "mse");

            if (dateString != null) {
                toDruid.put("timestamp", new DateTime(dateString).withZone(DateTimeZone.UTC).getMillis() / 1000);
            } else {
                toDruid.put("timestamp", System.currentTimeMillis() / 1000);
            }

            if (macAddress != null) store.put(macAddress + deployment_id, toCache);
            counter.inc();
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, toDruid));
        }
    }
}
