package io.connector.mysql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;

import java.util.List;
import java.util.ArrayList;

public class jdbcUtil {
    public static synchronized List<String> getAllTables(Connection conn) {
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            ResultSet rs = metadata.getTables(null, null, "%", null);
            List<String> tables = new ArrayList<String>();
            while (rs.next()) {
                String table = rs.getString("TABLE_NAME");
                tables.add(table);
            }

            return tables;
        }
        catch (SQLException e) {
            throw new traceMessage(e);
        }
    }

    public static <T> String join(Iterable<T> elements, String delim) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (T element : elements) {
            if (first) {
                first = false;
            }
            else {
                result.append(delim);
            }
            result.append(element);
        }
        return result.toString();
    }
}
