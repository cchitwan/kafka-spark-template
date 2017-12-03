package com.github.cchitwan.template;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@NoArgsConstructor
@Data
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

    public void closeConnection(Connection con) throws SQLException {
        con.close();
    }

}
