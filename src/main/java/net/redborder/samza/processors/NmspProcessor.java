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
import net.redborder.samza.util.constants.DimensionValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NmspProcessor implements IProcessor {
    final private static String NMSP_STORE = "rb_nmsp";
    private static NmspProcessor instance = null;

    private NmspProcessor() {}

    public NmspProcessor getInstance() {
        if (instance == null) instance = new NmspProcessor();
        return instance;
    }

    @Override
    public Map<String, Object> process(Map<String, Object> message) {
        Map<String, Object> toCache = new HashMap<>();
        String type = (String) message.get(Dimension.NMSP_TYPE);
        String mac = (String) message.get(Dimension.CLIENT_MAC);
        List<String> wireless_stations;
        String wireless_station;

        if (type != null && type.equals(DimensionValue.NMSP_TYPE_MEASURE)) {
            wireless_stations = (List<String>) message.get(Dimension.NMSP_AP_MAC);

            if (wireless_stations != null && !wireless_stations.isEmpty()) {
                wireless_station = wireless_stations.get(0);
                toCache.put(Dimension.WIRELESS_STATION, wireless_station);
            }
        }

        StoreManager.getStore(NMSP_STORE).put(mac, toCache);
        return null;
    }
}
