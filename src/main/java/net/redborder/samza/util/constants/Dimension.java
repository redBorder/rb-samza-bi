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

package net.redborder.samza.util.constants;

public class Dimension {
    // Common
    public final static String CLIENT_MAC = "client_mac";
    public final static String WIRELESS_STATION = "wireless_station";
    public final static String WIRELESS_ID = "wireless_id";
    public final static String SRC_IP = "src";
    public final static String SENSOR_NAME = "sensor_name";
    public final static String CLIENT_LATLNG = "client_latlong";
    public final static String CLIENT_RSSI = "client_rssi";
    public final static String CLIENT_RSSI_NUM = "client_rssi_num";
    public final static String CLIENT_SNR = "client_snr";
    public final static String CLIENT_SNR_NUM = "client_snr_num";
    public final static String TIMESTAMP = "timestamp";
    public final static String PKTS = "pkts";
    public final static String BYTES = "bytes";
    public final static String TYPE = "type";
    public final static String SRC_VLAN = "src_vlan";
    public final static String CAMPUS = "client_campus";
    public final static String BUILDING = "client_building";
    public final static String FLOOR = "client_floor";
    public final static String DOT11STATUS = "dot11_status";
    public final static String SRC = "src";

    // NMSP
    public final static String NMSP_AP_MAC = "ap_mac";
    public final static String NMSP_RSSI = "rssi";
    public final static String NMSP_DOT11STATUS = "dot11Status";
    public final static String NMSP_VLAN_ID = "vlan_id";

    // Location
    public final static String LOC_STREAMING_NOTIFICATION = "StreamingNotification";
    public final static String LOC_LOCATION = "location";
    public final static String LOC_GEOCOORDINATEv8 = "GeoCoordinate";
    public final static String LOC_GEOCOORDINATEv9 = "geoCoordinate";
    public final static String LOC_MAPINFOv8 = "MapInfo";
    public final static String LOC_MAPINFOv9 = "mapInfo";
    public final static String LOC_MACADDR = "macAddress";
    public final static String LOC_MAP_HIERARCHY = "mapHierarchyString";
    public final static String LOC_DOT11STATUS = "dot11Status";
    public final static String LOC_SSID = "ssId";
    public final static String LOC_IPADDR = "ipAddress";
    public final static String LOC_AP_MACADDR = "apMacAddress";
    public final static String LOC_SUBSCRIPTION_NAME = "subscriptionName";
    public final static String LOC_LONGITUDE = "longitude";
    public final static String LOC_LATITUDEv8 = "latitude";
    public final static String LOC_LATITUDEv9 = "lattitude";
}
