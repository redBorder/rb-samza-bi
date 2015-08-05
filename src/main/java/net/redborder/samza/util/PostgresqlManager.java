package net.redborder.samza.util;


import net.redborder.samza.store.StoreManager;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class PostgresqlManager {

    private static final String POSTGRESQL_STORE = "postgresql";
    private static final Logger log = LoggerFactory.getLogger(PostgresqlManager.class);
    private static final String[] enrichColumns = { "campus", "building", "floor", "deployment",
            "namespace", "market", "organization", "service_provider", "zone", "campus_uuid",
            "building_uuid", "floor_uuid", "deployment_uuid", "namespace_uuid", "market_uuid",
            "organization_uuid", "service_provider_uuid" };

    private static Connection conn = null;
    private static KeyValueStore<String, Map<String, Object>> storePostgreSql;

    public static void init(Config config, StoreManager storeManager){
        if(conn == null) {
            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            String uri = config.get("redborder.postgresql.uri");
            String user = config.get("redborder.postgresql.user");
            String pass = config.get("redborder.postgresql.pass");
            storePostgreSql = storeManager.getStore(POSTGRESQL_STORE);

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
    }

    public synchronized static Map<String, String> getSPSalt(){
        return new HashMap<>();
    }

    public synchronized static void update() {
        Statement st = null;
        ResultSet rs = null;
        long entries = 0L;

        try {
            if(conn != null) {
                st = conn.createStatement();
                rs = st.executeQuery("SELECT DISTINCT ON (access_points.mac_address) access_points.ip_address," +
                    "  access_points.mac_address, access_points.enrichment, zones.name AS zone, access_points.latitude AS latitude," +
                    "  access_points.longitude AS longitude, floors.name AS floor, floors.uuid AS floor_uuid," +
                    "  buildings.name AS building, buildings.uuid AS building_uuid, campuses.name AS campus," +
                    "  campuses.uuid AS campus_uuid, deployments.name AS deployment, deployments.uuid AS" +
                    "  deployment_uuid, namespaces.name AS namespace, namespaces.uuid AS namespace_uuid, " +
                    "  markets.name AS market, markets.uuid AS market_uuid, organizations.name AS organization," +
                    "  organizations.uuid AS organization_uuid, service_providers.name AS service_provider," +
                    "  service_providers.uuid AS service_provider_uuid FROM access_points" +
                    "  JOIN sensors ON access_points.sensor_id = sensors.id" +
                    "  LEFT JOIN access_points_zones AS zones_ids ON access_points.id = zones_ids.access_point_id" +
                    "  LEFT JOIN zones ON zones_ids.zone_id = zones.id" +
                    "  LEFT JOIN (SELECT * FROM sensors WHERE domain_type=101) AS floors" +
                    "    ON floors.lft <= sensors.lft AND floors.rgt >= sensors.rgt" +
                    "  LEFT JOIN (SELECT * FROM sensors WHERE domain_type=5) AS buildings" +
                    "    ON buildings.lft <= sensors.lft AND buildings.rgt >= sensors.rgt" +
                    "  LEFT JOIN (SELECT * FROM sensors WHERE domain_type=4) AS campuses" +
                    "    ON campuses.lft <= sensors.lft AND campuses.rgt >= sensors.rgt" +
                    "  LEFT JOIN (SELECT * FROM sensors WHERE domain_type=7) AS deployments" +
                    "    ON deployments.lft <= sensors.lft AND deployments.rgt >= sensors.rgt" +
                    "  LEFT JOIN (SELECT * FROM sensors WHERE domain_type=8) AS namespaces" +
                    "    ON namespaces.lft <= sensors.lft AND namespaces.rgt >= sensors.rgt" +
                    "  LEFT JOIN (SELECT * FROM sensors WHERE domain_type=3) AS markets" +
                    "    ON markets.lft <= sensors.lft AND markets.rgt >= sensors.rgt" +
                    "  LEFT JOIN (SELECT * FROM sensors WHERE domain_type=2) AS organizations" +
                    "    ON organizations.lft <= sensors.lft AND organizations.rgt >= sensors.rgt" +
                    "  LEFT JOIN (SELECT * FROM sensors WHERE domain_type=6) AS service_providers" +
                    "    ON service_providers.lft <= sensors.lft AND service_providers.rgt >= sensors.rgt");

                ObjectMapper mapper = new ObjectMapper();

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
                        storePostgreSql.put(rs.getString("mac_address"), location);
                    }
                }

                log.info("PostgreSql updated! Entries: " + entries);
            } else {
                log.warn("You must init the DB connection first!");
            }
        } catch (SQLException e) {
            log.error("The postgreSQL query failed! " + e.toString());
            e.printStackTrace();
        } finally {
            try { if (rs != null) rs.close(); } catch (Exception e) {}
            try { if (st != null) st.close(); } catch (Exception e) {}
        }
    }
}
