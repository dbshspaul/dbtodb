package com.sys.org.connecton;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ConnCreator implements AutoCloseable {
    Map<String, Connection> connectionMap = new HashMap<>();

    public Connection getConnection(String host, String port, String dbname, String username, String password) {
        Driver driver = new org.postgresql.Driver();
        Connection connection = connectionMap.get(dbname);
        if (connection == null) {
            try {
                DriverManager.registerDriver(driver);
                connection = DriverManager.getConnection(
                        "jdbc:postgresql://" + host + ":" + port + "/" + dbname, username, password);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            connectionMap.put(dbname, connection);
        }
        return connection;
    }

    public void close() throws Exception {
        for (Connection connection : connectionMap.values()) {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
