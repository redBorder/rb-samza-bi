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

        try {
            if(conn != null) {
                st = conn.createStatement();
                rs = st.executeQuery("SELECT access_points.ip_address, access_points.mac_address, access_points.latitude AS latitude, access_points.longitude AS longitude, floor.name AS floor_name,building.name AS building_name, campus.name AS campus_name, zones.name AS zone_name FROM access_points FULL OUTER JOIN sensors AS floor ON floor.id = access_points.sensor_id FULL OUTER JOIN sensors AS building ON floor.parent_id = building.id FULL OUTER JOIN sensors AS campus ON building.parent_id = campus.id FULL OUTER JOIN (SELECT MIN(zone_id) AS zone_id, access_point_id FROM access_points_zones GROUP BY access_point_id) AS zones_ids ON access_points.id = zones_ids.access_point_id FULL OUTER JOIN zones ON zones_ids.zone_id = zones.id WHERE access_points.mac_address IS NOT NULL;");
                long entries = 0L;
                while (rs.next()) {
                    Map<String, Object> location = new HashMap<>();
                    entries++;

                    if (rs.getString("longitude") != null && rs.getString("latitude") != null) {
                        Double longitude = (double) Math.round(Double.valueOf(rs.getString("longitude")) * 100000) / 100000;
                        Double latitude = (double) Math.round(Double.valueOf(rs.getString("latitude")) * 100000) / 100000;
                        location.put("client_latlong", latitude + "," + longitude);
                    }

                    if (rs.getString("campus_name") != null)
                        location.put("client_campus", rs.getString("campus_name"));
                    else
                        location.put("client_campus", "unknown");

                    if (rs.getString("building_name") != null)
                        location.put("client_building", rs.getString("building_name"));
                    else
                        location.put("client_building", "unknown");

                    if (rs.getString("floor_name") != null)
                        location.put("client_floor", rs.getString("floor_name"));
                    else
                        location.put("client_floor", "unknown");

                    if (rs.getString("zone_name") != null)
                        location.put("client_zone", rs.getString("zone_name"));
                    else
                        location.put("client_zone", "unknown");


                    if (!location.isEmpty())
                        storePostgreSql.put(rs.getString("mac_address"), location);
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
