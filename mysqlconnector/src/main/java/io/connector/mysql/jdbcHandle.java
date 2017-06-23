package io.connector.mysql;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;

public class jdbcHandle {
    private final int VALIDITY_TIMEOUT = 3;

    private String sqlUrl;
    private String sqlUser;
    private String sqlPasswd;
    private Connection sqlConnection = null;

    public jdbcHandle(String url, String user, String passwd) {
        this.sqlUrl = url;
        this.sqlUser = user;
        this.sqlPasswd = passwd;
    }

    public Connection getDBConnection() {
        try {
            if (sqlConnection == null) {
                sqlConnection = DriverManager.getConnection(sqlUrl, sqlUser, sqlPasswd);
            }
            else if (!sqlConnection.isValid(VALIDITY_TIMEOUT)) {
                closeConnection();
                sqlConnection = DriverManager.getConnection(sqlUrl, sqlUser, sqlPasswd);
            }
        }
        catch (SQLException e) {
            throw new traceMessage(e);
        }
        return sqlConnection;
    }

    public synchronized void closeConnection() {
        if (sqlConnection != null) {
            try {
                sqlConnection.close();
                sqlConnection = null;
            }
            catch (SQLException e) {
                throw new traceMessage(e);
            }
        }
    }
}
