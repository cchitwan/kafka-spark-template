package com.github.cchitwan.template;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@NoArgsConstructor
@Data
@Slf4j
public class PhoenixConnectionManager
        implements Serializable {

    private String connectionString;

    public PhoenixConnectionManager(String connectionString) {
        this.connectionString = connectionString;
    }

    public Connection getConnection() throws SQLException {
        Connection con = DriverManager.getConnection(connectionString);
        return con;
    }

    public boolean isConnectionValid(int timeoutSeconds) {
        try (Connection con = getConnection()) {
            return con != null && con.isValid(timeoutSeconds);
        } catch (SQLException e) {
            log.warn("Connection validation failed", e);
            return false;
        }
    }

    public void closeConnectionSilently(Connection con) {
        if (con == null) return;
        try {
            con.close();
        } catch (SQLException e) {
            log.warn("Error closing connection", e);
        }
    }

}
