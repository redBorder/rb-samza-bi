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

import net.redborder.samza.store.StoreManager;
import static net.redborder.samza.util.constants.Dimension.*;
import static net.redborder.samza.util.constants.DimensionValue.*;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NmspProcessor extends Processor {
    final public static String NMSP_STORE_MEASURE = "nmsp-measure";
    final public static String NMSP_STORE_INFO = "nmsp-info";


    private KeyValueStore<String, Map<String, Object>> storeMeasure;
    private KeyValueStore<String, Map<String, Object>> storeInfo;


    public NmspProcessor(StoreManager storeManager) {
        storeMeasure = storeManager.getStore(NMSP_STORE_MEASURE);
        storeInfo = storeManager.getStore(NMSP_STORE_INFO);
    }

    @Override
    public Map<String, Object> process(Map<String, Object> message) {
        Map<String, Object> toCache = new HashMap<>();
        Map<String, Object> toDruid = new HashMap<>();

        String type = (String) message.get(TYPE);
        String mac = (String) message.remove(CLIENT_MAC);

        if (type != null && type.equals(NMSP_TYPE_MEASURE)) {
            List<String> apMacs = (List<String>) message.get(NMSP_AP_MAC);
            List<Integer> clientRssis = (List<Integer>) message.get(NMSP_RSSI);

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
                String dot11Status = "PROBING";

                if (infoCache == null) {
                    toCache.put(CLIENT_RSSI_NUM, rssi);
                    toCache.put(WIRELESS_STATION, apMac);
                    toCache.put(NMSP_DOT11STATUS, "ASSOCIATED");
                    dot11Status = "PROBING";
                } else {
                    String apAssociated = (String) toCache.get(WIRELESS_STATION);

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
            } else if (type != null && type.equals(NMSP_TYPE_INFO)) {

                Object vlan = message.remove(NMSP_VLAN_ID);

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
            }
        }

        return toDruid;
    }
}
