package io.connector.mysql;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;

public class mysqlSourceConnector extends SourceConnector {
    public static final String TOPIC_CONFIG = "topics";
    public static final String MYSQL_URL = "mysql.url";
    public static final String MYSQL_USER = "mysql.user";
    public static final String MYSQL_PASSWD = "mysql.passwd";
    public static final String BATCH_SIZE = "batch.size";
    public static final String TABLE_NAME = "table.name";

    private String topic;
    private String mysqlUrl;
    private String mysqlUser;
    private String mysqlPasswd;
    private String tableName;
    private jdbcHandle dbHandle;
    //private List<String> tables;
    private final String batchSize = "5";

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(TOPIC_CONFIG);
        if (topic == null || topic.isEmpty()) {
            throw new ConnectException("MysqlSourceConnector must include 'topic' setting.");
        }
        if (topic.contains(",")) {
            throw new ConnectException("MysqlSourceConnector should only have a single topic.");
        }

        mysqlUrl = props.get(MYSQL_URL);
        if (mysqlUrl == null || mysqlUrl.isEmpty()) {
            throw new ConnectException("MysqlSourceConnector must include 'mysql.url' setting.");
        }

        mysqlUser = props.get(MYSQL_USER);
        if (mysqlUser == null || mysqlUser.isEmpty()) {
            throw new ConnectException("MysqlSourceConnector must include 'mysql.user' setting.");
        }

        mysqlPasswd = props.get(MYSQL_PASSWD);
        if (mysqlPasswd == null || mysqlPasswd.isEmpty()) {
            throw new ConnectException("MysqlSourceConnector must include 'mysql.passwd' setting.");
        }

        tableName= props.get(TABLE_NAME);
        if (tableName == null || tableName.isEmpty()) {
            throw new ConnectException("MysqlSourceConnector must include 'table.name' setting.");
        }

        dbHandle = new jdbcHandle(mysqlUrl, mysqlUser, mysqlPasswd);
        //tables = jdbcUtil.getAllTables(dbHandle.getDBConnection());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return mysqlSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();

        config.put(TOPIC_CONFIG, topic);
        config.put(MYSQL_URL, mysqlUrl);
        config.put(MYSQL_USER, mysqlUser);
        config.put(MYSQL_PASSWD, mysqlPasswd);
        config.put(BATCH_SIZE, batchSize);
        config.put(TABLE_NAME, tableName);
        //config.put(TABLE_LIST, jdbcUtil.join(tables, ","));
        configs.add(config);

        return configs;
    }

    @Override
    public void stop() {
        dbHandle.closeConnection();
    }
}
