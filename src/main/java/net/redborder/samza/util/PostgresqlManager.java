package net.redborder.samza.util;


import net.redborder.samza.store.StoreManager;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class PostgresqlManager {

    private static final String POSTGRESQL_STORE = "postgresql";
    private static final Logger log = LoggerFactory.getLogger(PostgresqlManager.class);

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
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized static void update() {
        Statement st = null;
        ResultSet rs = null;
        long entries = 0L;

        try {
            if(conn != null) {
                st = conn.createStatement();
                rs = st.executeQuery("SELECT DISTINCT ON (access_points.mac_address) access_points.ip_address," +
                    "  access_points.mac_address, zones.name AS zone, access_points.latitude AS latitude," +
                    "  access_points.longitude AS longitude, floors.name AS floor, floors.uuid AS floor_id," +
                    "  buildings.name AS building, buildings.uuid AS building_id, campuses.name AS campus," +
                    "  campuses.uuid AS campus_id, deployments.name AS deployment, deployments.uuid AS" +
                    "  deployment_id, namespaces.name AS namespace, namespaces.uuid AS namespace_id, " +
                    "  markets.name AS market, markets.uuid AS market_id, organizations.name AS organization," +
                    "  organizations.uuid AS organization_id, service_providers.name AS service_provider," +
                    "  service_providers.uuid AS service_provider_id FROM access_points" +
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

                while (rs.next()) {
                    Map<String, Object> location = new HashMap<>();
                    Map<String, String> enriching = new HashMap<>();
                    String longitude = rs.getString("longitude");
                    String latitude = rs.getString("latitude");

                    enriching.put("campus", rs.getString("campus"));
                    enriching.put("building", rs.getString("building"));
                    enriching.put("floor", rs.getString("floor"));
                    enriching.put("deployment", rs.getString("deployment"));
                    enriching.put("namespace", rs.getString("namespace"));
                    enriching.put("market", rs.getString("market"));
                    enriching.put("organization", rs.getString("organization"));
                    enriching.put("service_provider", rs.getString("service_provider"));
                    enriching.put("zone", rs.getString("zone"));

                    enriching.put("campus_id", rs.getString("campus_id"));
                    enriching.put("building_id", rs.getString("building_id"));
                    enriching.put("floor_id", rs.getString("floor_id"));
                    enriching.put("deployment_id", rs.getString("deployment_id"));
                    enriching.put("namespace_id", rs.getString("namespace_id"));
                    enriching.put("market_id", rs.getString("market_id"));
                    enriching.put("organization_id", rs.getString("organization_id"));
                    enriching.put("service_provider_id", rs.getString("service_provider_id"));

                    entries++;

                    if (longitude != null && latitude != null) {
                        Double longitudeDbl = (double) Math.round(Double.valueOf(longitude) * 100000) / 100000;
                        Double latitudeDbl = (double) Math.round(Double.valueOf(latitude) * 100000) / 100000;
                        location.put("client_latlong", latitudeDbl + "," + longitudeDbl);
                    }

                    for (Map.Entry<String, String> entry : enriching.entrySet()) {
                        String value = entry.getValue();
                        String key = entry.getKey();

                        if (value != null) {
                            location.put(key, value);
                        } else {
                            location.put(key, "unknown");
                        }
                    }

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
