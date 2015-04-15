package net.redborder.samza.util;

import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by andresgomez on 19/12/14.
 */
public class PostgresqlManager {
    private static Connection conn = null;
    private static PostgresqlManager instance = null;
    private static String _user;
    private static String _uri;
    private static String _pass;
    private static Logger logger = LoggerFactory.getLogger(PostgresqlManager.class);
    private static Map<String, String> _hash;
    private KeyValueStore<String, SystemStream> store;

    private PostgresqlManager(KeyValueStore<String, SystemStream> store, String uri, String user, String pass) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        initConfig(uri, user, pass);
        init(store);
    }

    public static void initConfig(String uri, String user, String pass) {
        _uri = uri;
        _user = user;
        _pass = pass;
    }

    public static PostgresqlManager getInstance(KeyValueStore<String, SystemStream> store, String uri, String user, String pass) {
        if (_uri != null && _user != null) {
            if (instance == null) {
                instance = new PostgresqlManager(store, uri, user, pass);
            }
        } else {
            System.out.println("You must call initConfig first!");
        }
        return instance;
    }

    private void initConnection() {
        try {
            if (_uri != null && _user != null)
                conn = DriverManager.getConnection(_uri, _user, _pass);
            else
                System.out.println("You must initialize the db_uri and db_user at bi_config file.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void init(KeyValueStore<String, SystemStream> store) {
        if (conn == null) {
            initConnection();
            _hash = new HashMap<>();
        }
    }

    private synchronized void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void close() {
        closeConnection();
    }

    public static String get(String tenantID) {
        String dataSource = _hash.get(tenantID);

        if(dataSource == null){
            updateTenant();
            dataSource = _hash.get(tenantID);
        }

        return dataSource;
    }

    public synchronized static void updateTenant() {
        Map<String, String> map = new HashMap<>();
        Statement st = null;
        ResultSet rs = null;

        try {
            if(conn != null) {
                st = conn.createStatement();
                rs = st.executeQuery("");

                while (rs.next()) {
                        map.put(rs.getString("tenant_id"), rs.getString("data_source"));
                }
            }
            logger.info("Location Entry: " + _hash.size() + " \n   Updated data: \n " + _hash.toString());
        } catch (SQLException e) {
            logger.error("The postgreSQL query failed! " + e.toString());
            e.printStackTrace();
        } finally {
            try { if (rs != null) rs.close(); } catch (Exception e) {}
            try { if (st != null) st.close(); } catch (Exception e) {}
        }
        _hash = map;
    }
}
