package io.connector.mysql;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;

import java.util.Map;
import java.util.Collections;

public class jdbcQuerier {
    private String topic;
    private String name;
    private Integer lastOffset;
    private ResultSet resultSet;
    private PreparedStatement stm;
    private Schema schema;

    public jdbcQuerier(String topic, String name, Map<String, Object> offsetMap) {
        this.topic = topic;
        this.name = name;
        this.lastOffset = (offsetMap == null ? null : (Integer)offsetMap.get("position"));
        this.resultSet = null;
        this.schema = null;
    }

    public String getQuerierName() {
        return name;
    }

    public void startQuery(Connection dbConn) {
        try {
            if (resultSet == null) {
                getStatement(dbConn);
                resultSet = stm.executeQuery();
                schema = dataConvertor.convertSchema("table", resultSet.getMetaData());
            }
        }
        catch (SQLException e) {
            throw new traceMessage(e);
        }
    }

    public SourceRecord extractRecord() {
        try {
            Struct struct = dataConvertor.convertRecord(schema, resultSet);
            lastOffset = struct.getInt32("id");

            Map<String, String> partition = Collections.singletonMap("table", name);
            Map<String, Integer> offset = Collections.singletonMap("position", lastOffset);
            SourceRecord record = new SourceRecord(partition, offset, topic, struct.schema(), struct);
            return record;
        }
        catch (SQLException e) {
            throw new traceMessage(e);
        }
    }

    public void reset() {
        closeResultSet();
        closeStatement();
        schema = null;
    }

    public boolean hasNext() {
        try {
            return resultSet.next();
        }
        catch (SQLException e) {
            throw new traceMessage(e);
        }
    }

    private PreparedStatement getStatement(Connection dbConn) {
        try {
            String sql = getQuerySql();
            stm = dbConn.prepareStatement(sql);
            return stm;
        }
        catch (SQLException e) {
            throw new traceMessage(e);
        }
    }

    private String getQuerySql() {
        String sql;
        if (lastOffset == null) {
            sql = "select * from " + name;
        }
        else {
            sql = "select * from " + name + " where id > " + lastOffset;
        }
        System.out.println("sql: " + sql);
        return sql;
    }

    private void closeResultSet() {
        if (resultSet != null) {
            try {
                resultSet.close();
            }
            catch (SQLException e) {
            }
            resultSet = null;
        }
    }

    private void closeStatement() {
        if (stm != null) {
            try {
                stm.close();
            }
            catch (SQLException e) {
            }
            stm = null;
        }
    }
}
