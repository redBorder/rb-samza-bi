package net.redborder.samza.util.constants;

public class Dimension {
    // Common
    public final static String CLIENT_MAC = "client_mac";
    public final static String WIRELESS_STATION = "wireless_station";
    public final static String WIRELESS_ID = "wireless_id";
    public final static String SRC_IP = "src_ip";
    public final static String SENSOR_IP = "sensor_ip";
    public final static String DST_IP = "dst_ip";
    public final static String SENSOR_NAME = "sensor_name";
    public final static String CLIENT_LATLNG = "client_latlong";
    public final static String CLIENT_PROFILE = "client_profile";
    public final static String CLIENT_RSSI = "client_rssi";
    public final static String CLIENT_RSSI_NUM = "client_rssi_num";
    public final static String CLIENT_SNR = "client_snr";
    public final static String CLIENT_SNR_NUM = "client_snr_num";
    public final static String TIMESTAMP = "timestamp";
    public final static String FIRST_SWITCHED = "first_switched";
    public final static String DURATION = "duration";
    public final static String PKTS = "pkts";
    public final static String BYTES = "bytes";
    public final static String TYPE = "type";
    public final static String SRC_VLAN = "src_vlan";
    public final static String DST_VLAN = "dst_vlan";
    public final static String WAN_VLAN = "wan_vlan";
    public final static String LAN_VLAN = "lan_vlan";
    public final static String DOT11STATUS = "dot11_status";
    public final static String CLIENT_MAC_VENDOR = "client_mac_vendor";
    public final static String CLIENT_ID = "client_id";
    public final static String SRC_AS_NAME = "src_as_name";
    public final static String SRC_AS = "src_as";
    public final static String LAN_IP_AS_NAME = "lan_ip_as_name";
    public final static String SRC_PORT = "src_port";
    public final static String LAN_L4_PORT = "lan_l4_port";
    public final static String SRC_MAP = "src_map";
    public final static String SRV_PORT = "srv_port";
    public final static String DST_AS_NAME = "dst_as_name";
    public final static String WAN_IP_AS_NAME = "wan_ip_as_name";
    public final static String DST_PORT = "dst_port";
    public final static String WAN_L4_PORT = "wan_l4_port";
    public final static String DST_MAP = "dst_map";
    public final static String DST_AS = "dst_as";
    public final static String ZONE_UUID = "zone_uuid";
    public final static String APPLICATION_ID_NAME = "application_id_name";
    public final static String BIFLOW_DIRECTION = "biflow_direction";
    public final static String CONVERSATION = "conversation";
    public final static String DIRECTION = "direction";
    public final static String ENGINE_ID_NAME = "engine_id_name";
    public final static String HTTP_HOST = "host";
    public final static String HTTP_SOCIAL_MEDIA = "http_social_media";
    public final static String HTTP_SOCIAL_USER = "http_social_user";
    public final static String HTTP_USER_AGENT_OS = "http_user_agent";
    public final static String HTTP_REFER_L1 = "referer";
    public final static String IP_PROTOCOL_VERSION = "ip_protocol_version";
    public final static String L4_PROTO = "l4_proto";
    public final static String LAN_IP_NET_NAME = "lan_ip_net_name";
    public final static String SRC_NET_NAME = "src_net_name";
    public final static String WAN_IP_NET_NAME = "wan_ip_net_name";
    public final static String DST_NET_NAME = "dst_net_name";
    public final static String TOS = "tos";
    public final static String DST_COUNTRY_CODE = "dst_country_code";
    public final static String WAN_IP_COUNTRY_CODE = "wan_ip_country_code";
    public final static String SRC_COUNTRY_CODE = "src_country_code";
    public final static String SRC_COUNTRY = "src_country";
    public final static String DST_COUNTRY = "dst_country";
    public final static String LAN_IP_COUNTRY_CODE = "lan_ip_country_code";
    public final static String SCATTERPLOT = "scatterplot";
    public final static String INPUT_SNMP = "lan_interface_name";
    public final static String OUTPUT_SNMP = "wan_interface_name";
    public final static String INPUT_VRF = "input_vrf";
    public final static String OUTPUT_VRF = "output_vrf";
    public final static String SERVICE_PROVIDER = "service_provider";
    public final static String SERVICE_PROVIDER_UUID = "service_provider_uuid";
    public final static String SRC = "src";
    public final static String LAN_IP = "lan_ip";

    public final static String BUILDING = "building";
    public final static String BUILDING_UUID = "building_uuid";
    public final static String CAMPUS = "campus";
    public final static String CAMPUS_UUID = "campus_uuid";
    public final static String FLOOR = "floor";
    public final static String FLOOR_UUID = "floor_uuid";
    public final static String ZONE = "zone";

    public final static String COORDINATES_MAP = "coordinates_map";
    public final static String HNBLOCATION = "hnblocation";
    public final static String HNBGEOLOCATION = "hnbgeolocation";
    public final static String RAT = "rat";
    public final static String DOT11PROTOCOL = "dot11_protocol";
    public final static String DEPLOYMENT = "deployment";
    public final static String DEPLOYMENT_UUID = "deployment_uuid";
    public final static String NAMESPACE = "namespace";
    public final static String NAMESPACE_UUID = "namespace_uuid";
    public final static String TIER = "tier";
    public final static String MSG = "msg";
    public final static String HTTPS_COMMON_NAME = "https_common_name";
    public final static String TARGET_NAME = "target_name";

    public final static String CLIENT_FULLNAME = "client_fullname";
    public final static String PRODUCT_NAME = "product_name";

    // Malware
    public static final String HASH_SCORE = "hash_score";
    public static final String IP_SCORE = "ip_score";
    public static final String URL_SCORE = "url_score";
    public static final String PROBE_HASH_SCORE = "hash_probe_score";
    public static final String PROBE_IP_SCORE = "ip_probe_score";
    public static final String PROBE_URL_SCORE = "url_probe_score";

    public static final String URL = "url";
    public static final String FILE_NAME = "file_name";
    public static final String EMAIL_SENDER = "email_sender";
    public static final String EMAIL_DESTINATION = "email_destination";
    public static final String EMAIL_ID = "email_id";

    // Event
    public final static String ACTION = "action";
    public final static String CLASSIFICATION = "classification";
    public final static String DOMAIN_NAME = "domain_name";
    public final static String ETHLENGTH_RANGE = "ethlength_range";
    public final static String GROUP_NAME = "group_name";
    public final static String SIG_GENERATOR = "sig_generator";
    public final static String ICMPTYPE = "icmptype";
    public final static String IPLEN_RANGE = "iplen_range";
    public final static String REV = "rev";
    public final static String SENSOR_UUID = "sensor_uuid";
    public final static String PRIORITY = "priority";
    public final static String SIG_ID = "sig_id";
    public final static String ETHSRC = "ethsrc";
    public final static String ETHSRC_VENDOR = "ethsrc_vendor";
    public final static String ETHDST = "ethdst";
    public final static String ETHDST_VENDOR = "ethdst_vendor";
    public final static String DST = "dst";
    public final static String WAN_IP = "wan_ip";
    public final static String TTL = "ttl";
    public final static String VLAN = "vlan";
    public final static String MARKET = "market";
    public final static String MARKET_UUID = "market_uuid";
    public final static String ORGANIZATION = "organization";
    public final static String ORGANIZATION_UUID = "organization_uuid";
    public final static String CLIENT_LATLONG = "client_latlong";
    public final static String HASH = "hash";
    public final static String FILE_SIZE = "file_size";
    public final static String SHA256 = "sha256";
    public final static String FILE_URI = "file_uri";
    public final static String FILE_HOSTNAME = "file_hostname";
    public final static String GROUP_UUID = "group_uuid";
    public final static String CLIENT_NAME = "client_name";

    // Darklist
    public final static String DARKLIST_SCORE = "darklist_score";
    public final static String DARKLIST_SCORE_NAME = "darklist_score_name";
    public final static String DARKLIST_PROTOCOL = "darklist_protocol";
    public final static String DARKLIST_DIRECTION = "darklist_direction";
    public final static String DARKLIST_CATEGORY = "darklist_category";

    // NMSP
    public final static String NMSP_AP_MAC = "ap_mac";
    public final static String NMSP_RSSI = "rssi";
    public final static String NMSP_DOT11STATUS = "dot11_status";
    public final static String NMSP_VLAN_ID = "vlan_id";
    public final static String NMSP_DOT11PROTOCOL = "dot11_protocol";
    public final static String NMSP_WIRELESS_ID = "wireless_id";

    // Location
    public final static String LOC_TIMESTAMP_MILLIS = "timestampMillis";
    public final static String SSID = "ssid";
    public final static String LOC_MSEUDI = "mseUdi";
    public final static String LOC_NOTIFICATIONS = "notifications";
    public final static String LOC_NOTIFICATION_TYPE = "notificationType";
    public final static String LOC_STREAMING_NOTIFICATION = "StreamingNotification";
    public final static String LOC_LOCATION = "location";
    public final static String LOC_GEOCOORDINATEv8 = "GeoCoordinate";
    public final static String LOC_GEOCOORDINATEv9 = "geoCoordinate";
    public final static String LOC_MAPINFOv8 = "MapInfo";
    public final static String LOC_MAPINFOv9 = "mapInfo";
    public final static String LOC_MAPCOORDINATEv8 = "MapCoordinate";
    public final static String LOC_MACADDR = "macAddress";
    public final static String LOC_MAP_HIERARCHY = "mapHierarchyString";
    public final static String LOC_MAP_HIERARCHY_V10 = "locationMapHierarchy";
    public final static String LOC_DOT11STATUS = "dot11Status";
    public final static String LOC_SSID = "ssId";
    public final static String LOC_IPADDR = "ipAddress";
    public final static String LOC_AP_MACADDR = "apMacAddress";
    public final static String LOC_SUBSCRIPTION_NAME = "subscriptionName";
    public final static String LOC_LONGITUDE = "longitude";
    public final static String LOC_LATITUDEv8 = "latitude";
    public final static String LOC_LATITUDEv9 = "lattitude";
    public final static String LOC_DEVICEID = "deviceId";
    public final static String LOC_BAND = "band";
    public final static String LOC_STATUS = "status";
    public final static String LOC_USERNAME = "username";
    public final static String LOC_ENTITY = "entity";
    public final static String LOC_COORDINATE = "locationCoordinate";
    public final static String LOC_COORDINATE_X = "x";
    public final static String LOC_COORDINATE_Y = "y";
    public final static String LOC_COORDINATE_Z = "z";
    public final static String LOC_COORDINATE_UNIT = "unit";
    public final static String WIRELESS_OPERATOR = "wireless_operator";
    public final static String CLIENT_OS = "client_os";
    public final static String INTERFACE_NAME = "interface_name";

    // State
    public final static String WIRELESS_CHANNEL = "wireless_channel";
    public final static String WIRELESS_TX_POWER = "wireless_tx_power";
    public final static String WIRELESS_ADMIN_STATE = "wireless_admin_state";
    public final static String WIRELESS_OP_STATE = "wireless_op_state";
    public final static String WIRELESS_MODE = "wireless_mode";
    public final static String WIRELESS_SLOT = "wireless_slot";
    public final static String WIRELESS_STATION_IP = "wireless_station_ip";
    public final static String STATUS = "status";
    public final static String WIRELESS_STATION_NAME = "wireless_station_name";

    //Hashtag
    public final static String VALUE = "value";

    // Social
    public final static String USER_SCREEN_NAME = "user_screen_name";
    public final static String USER_NAME_SOCIAL = "user_name";
    public final static String USER_ID = "user_id";
    public final static String HASHTAGS = "hashtags";
    public final static String MENTIONS = "mentions";
    public final static String SENTIMENT = "sentiment";
    public final static String MSG_SEND_FROM = "msg_send_from";
    public final static String USER_FROM = "user_from";
    public final static String USER_PROFILE_IMG_HTTPS = "user_profile_img_https";
    public final static String INFLUENCE = "influence";
    public final static String PICTURE_URL = "picture_url";
    public final static String LANGUAGE = "language";
    public final static String CATEGORY = "category";
    public final static String FOLLOWERS = "followers";

    //LOCATION
    public final static String OLD_LOC = "old";
    public final static String NEW_LOC = "new";
    public final static String DWELL_TIME = "dwell_time";
    public final static String TRANSITION = "transition";
    public final static String SESSION = "session";
    public final static String REPETITIONS = "repetitions";

    // Radius
    public final static String PACKET_SRC_IP_ADDRESS = "Packet-Src-IP-Address";
    public final static String USER_NAME_RADIUS = "User-Name";
    public final static String OPERATOR_NAME = "Operator-Name";
    public final static String AIRESPACE_WLAN_ID = "Airespace-Wlan_Id";
    public final static String CALLING_STATION_ID = "Calling-Station-Id";
    public final static String ACCT_STATUS_TYPE = "Acct-Status-Type";
    public final static String CALLED_STATION_ID = "Called-Station-Id";
    public final static String CLIENT_ACCOUNTING_TYPE = "client_accounting_type";

    // Pms
    public final static String GUEST_NAME = "guest_name";
    public final static String STAFF_NAME = "staff_name";
    public final static String CLIENT_GENDER = "client_gender";
    public final static String CLIENT_AUTH_TYPE = "client_auth_type";
    public final static String AUTH_TYPE = "auth_type";
    public final static String CLIENT_VIP = "client_vip";
    public final static String CLIENT_LOYALITY = "client_loyality";

    //Dwell
    public final static String WINDOW = "window";

    //ICAP
    public final static String PROXY_IP = "proxy_ip";

    //Vault
    public final static String PRI = "pri";
    public final static String PRI_TEXT = "pri_text";
    public final static String SYSLOG_FACILITY = "syslogfacility";
    public final static String SYSLOG_FACILITY_TEXT = "syslogfacility_text";
    public final static String SYSLOGSEVERITY = "syslogseverity";
    public final static String SYSLOGSEVERITY_TEXT = "syslogseverity_text";
    public final static String HOSTNAME = "hostname";
    public final static String FROMHOST_IP = "fromhost_ip";
    public final static String APP_NAME = "app_name";
    public final static String PROXY_UUID = "proxy_uuid";
    public final static String MESSAGE = "message";
    public final static String SOURCE = "source";
    public final static String TARGET = "target";
}
