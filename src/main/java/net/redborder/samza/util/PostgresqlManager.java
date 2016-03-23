package net.redborder.samza.util;


import net.redborder.samza.store.StoreManager;
import org.apache.commons.lang.StringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.bouncycastle.util.encoders.Hex;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresqlManager {

    public static final String WLC_PSQL_STORE = "wlc-psql";
    public static final String SENSOR_PSQL_STORE = "sensor-psql";
    private final Logger log = LoggerFactory.getLogger(PostgresqlManager.class);
    private final String[] enrichColumns = {"campus", "building", "floor", "deployment",
            "namespace", "market", "organization", "service_provider", "zone", "campus_uuid",
            "building_uuid", "floor_uuid", "deployment_uuid", "namespace_uuid", "market_uuid",
            "organization_uuid", "service_provider_uuid", "zone_uuid"};

    private Connection conn = null;
    private KeyValueStore<String, Map<String, Object>> storeWLCSql;
    private KeyValueStore<String, Map<String, Object>> storeSensorSql;
    private Map<String, MacScramble> scrambles = new HashMap<>();
    private String macScramblePrefix = null;

    public PostgresqlManager(Config config, StoreManager storeManager) {
        if (conn == null) {
            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            String uri = config.get("redborder.postgresql.uri");
            String user = config.get("redborder.postgresql.user");
            String pass = config.get("redborder.postgresql.pass");
            macScramblePrefix = config.get("redborder.macscramble.prefix");
            storeWLCSql = storeManager.getStore(WLC_PSQL_STORE);
            storeSensorSql = storeManager.getStore(SENSOR_PSQL_STORE);

            try {
                if (uri != null && user != null) {
                    conn = DriverManager.getConnection(uri, user, pass);
                } else {
                    log.warn("You must specify a URI, user and pass on the config file in order to use postgresql.");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        update();
        updateSalts();
    }


    public Map<String, MacScramble> getScrambles() {
        return scrambles;
    }

    public void updateSalts() {
        Statement st = null;
        ResultSet rs = null;
        scrambles.clear();

        try {
            if (conn != null) {
                st = conn.createStatement();
                rs = st.executeQuery("SELECT uuid, property FROM sensors WHERE domain_type=6;");
                ObjectMapper mapper = new ObjectMapper();
                while (rs.next()) {
                    String uuid = rs.getString("uuid");
                    String propertyStr = rs.getString("property");

                    Map<String, Object> properties = mapper.readValue(propertyStr, Map.class);
                    String salt = (String) properties.get("mac_hashing_salt");

                    if (salt != null) {
                        scrambles.put(uuid, new MacScramble(Hex.decode(salt), macScramblePrefix));
                    }
                }
            }

            log.info("Updated salts: {}", scrambles.entrySet());
        } catch (SQLException e) {
            log.error("The postgreSQL query failed! " + e.toString());
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) rs.close();
            } catch (Exception e) {
            }
            try {
                if (st != null) st.close();
            } catch (Exception e) {
            }
        }
    }

    public void update() {
        updateWLC();
        updateSensor();
    }

    public void updateSensor() {
        Statement st = null;
        ResultSet rs = null;

        try {
            if (conn != null) {
                st = conn.createStatement();
                rs = st.executeQuery("SELECT uuid, latitude, longitude FROM sensors");

                Map<String, Map<String, Object>> tmpCache = new HashMap<>();

                while (rs.next()) {
                    String uuid = rs.getString("uuid");
                    String longitude = rs.getString("longitude");
                    String latitude = rs.getString("latitude");

                    if (uuid != null && latitude != null && longitude != null
                            && StringUtils.isNumeric(latitude) && StringUtils.isNumeric(longitude)) {
                        Map<String, Object> location = new HashMap<>();
                        Double longitudeDbl = (double) Math.round(Double.valueOf(longitude) * 100000) / 100000;
                        Double latitudeDbl = (double) Math.round(Double.valueOf(latitude) * 100000) / 100000;
                        location.put("client_latlong", latitudeDbl + "," + longitudeDbl);

                        tmpCache.put(uuid, location);
                        storeSensorSql.put(uuid, location);
                    }
                }


                KeyValueIterator<String, Map<String, Object>> iterator = storeSensorSql.all();

                List<String> toRemove = new ArrayList<>();
                while (iterator.hasNext()) {
                    Entry<String, Map<String, Object>> entry = iterator.next();
                    String key = entry.getKey();

                    if (!tmpCache.containsKey(key)) {
                        toRemove.add(key);
                    }
                }

                for (String key : toRemove) {
                    storeSensorSql.delete(key);
                }
            }
        } catch (SQLException e) {
            log.error("The postgreSQL query failed! " + e.toString());
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) rs.close();
            } catch (Exception e) {
            }
            try {
                if (st != null) st.close();
            } catch (Exception e) {
            }
        }

    }

    public void updateWLC() {
        Statement st = null;
        ResultSet rs = null;
        long entries = 0L;

        try {
            if (conn != null) {
                st = conn.createStatement();
                rs = st.executeQuery("SELECT DISTINCT ON (access_points.mac_address) access_points.ip_address, access_points.mac_address, access_points.enrichment," +
                        " zones.name AS zone, zones.id AS zone_uuid, access_points.latitude AS latitude, access_points.longitude AS longitude, floors.name AS floor, " +
                        " floors.uuid AS floor_uuid, buildings.name AS building, buildings.uuid AS building_uuid, campuses.name AS campus, campuses.uuid AS campus_uuid," +
                        " deployments.name AS deployment, deployments.uuid AS deployment_uuid, namespaces.name AS namespace, namespaces.uuid AS namespace_uuid," +
                        " markets.name AS market, markets.uuid AS market_uuid, organizations.name AS organization, organizations.uuid AS organization_uuid," +
                        " service_providers.name AS service_provider, service_providers.uuid AS service_provider_uuid" +
                        " FROM access_points JOIN sensors ON (access_points.sensor_id = sensors.id)" +
                        " LEFT JOIN access_points_zones AS zones_ids ON access_points.id = zones_ids.access_point_id" +
                        " LEFT JOIN zones ON zones_ids.zone_id = zones.id" +
                        " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=101) AS floors ON floors.lft <= sensors.lft AND floors.rgt >= sensors.rgt" +
                        " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=5) AS buildings ON buildings.lft <= sensors.lft AND buildings.rgt >= sensors.rgt" +
                        " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=4) AS campuses ON campuses.lft <= sensors.lft AND campuses.rgt >= sensors.rgt" +
                        " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=7) AS deployments ON deployments.lft <= sensors.lft AND deployments.rgt >= sensors.rgt" +
                        " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=8) AS namespaces ON namespaces.lft <= sensors.lft AND namespaces.rgt >= sensors.rgt" +
                        " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=3) AS markets ON markets.lft <= sensors.lft AND markets.rgt >= sensors.rgt" +
                        " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=2) AS organizations ON organizations.lft <= sensors.lft AND organizations.rgt >= sensors.rgt" +
                        " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=6) AS service_providers ON service_providers.lft <= sensors.lft AND service_providers.rgt >= sensors.rgt");


                ObjectMapper mapper = new ObjectMapper();

                Map<String, Map<String, Object>> tmpCache = new HashMap<>();

                while (rs.next()) {
                    Map<String, Object> location = new HashMap<>();
                    Map<String, String> enriching = new HashMap<>();

                    String enrichmentStr = rs.getString("enrichment");
                    if (enrichmentStr != null) {
                        try {
                            Map<String, String> enrichment = mapper.readValue(enrichmentStr, Map.class);
                            enriching.putAll(enrichment);
                        } catch (JsonMappingException e) {
                            e.printStackTrace();
                        } catch (JsonParseException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    String longitude = rs.getString("longitude");
                    String latitude = rs.getString("latitude");

                    for (String columnName : enrichColumns) {
                        String columnData = rs.getString(columnName);
                        if (columnData != null) enriching.put(columnName, columnData);
                    }

                    entries++;

                    if (longitude != null && latitude != null) {
                        Double longitudeDbl = (double) Math.round(Double.valueOf(longitude) * 100000) / 100000;
                        Double latitudeDbl = (double) Math.round(Double.valueOf(latitude) * 100000) / 100000;
                        location.put("client_latlong", latitudeDbl + "," + longitudeDbl);
                    }

                    location.putAll(enriching);
                    if (!location.isEmpty()) {
                        log.debug("AP: {} LOCATION: {}", rs.getString("mac_address"), location);
                        tmpCache.put(rs.getString("mac_address"), location);
                        storeWLCSql.put(rs.getString("mac_address"), location);

                    }
                }

                KeyValueIterator<String, Map<String, Object>> iterator = storeWLCSql.all();

                List<String> toRemove = new ArrayList<>();
                while (iterator.hasNext()) {
                    Entry<String, Map<String, Object>> entry = iterator.next();
                    String key = entry.getKey();

                    if (!tmpCache.containsKey(key)) {
                        toRemove.add(key);
                    }
                }

                for (String key : toRemove) {
                    storeWLCSql.delete(key);
                    log.info("Removing {} from postgresqlStore", key);
                }

                log.info("PostgreSql updated! Entries: " + entries);
            } else {
                log.warn("You must init the DB connection first!");
            }
        } catch (SQLException e) {
            log.error("The postgreSQL query failed! " + e.toString());
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) rs.close();
            } catch (Exception e) {
            }
            try {
                if (st != null) st.close();
            } catch (Exception e) {
            }
        }
    }
}
