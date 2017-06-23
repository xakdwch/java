package io.connector.mysql;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class mysqlSinkConnector extends SinkConnector {
    public static String TOPIC_CONFIG = "topics";
    public static String MYSQL_URL = "mysql.url";
    public static String MYSQL_USER = "mysql.user";
    public static String MYSQL_PASSWD = "mysql.passwd";
    public static String TABLE_NAME = "table.name";

    private String topic;
    private String mysqlUrl;
    private String mysqlUser;
    private String mysqlPasswd;
    private String tableName;

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(TOPIC_CONFIG);
        if (topic == null || topic.isEmpty()) {
            throw new ConnectException("mysqlSinkConnector should include 'topic' setting.");
        }

        mysqlUrl = props.get(MYSQL_URL);
        if (mysqlUrl == null || mysqlUrl.isEmpty()) {
            throw new ConnectException("mysqlSinkConnector should include 'mysql.url' setting.");
        }

        mysqlUser = props.get(MYSQL_USER);
        if (mysqlUser == null || mysqlUser.isEmpty()) {
            throw new ConnectException("mysqlSinkConnector should include 'mysql.user' setting.");
        }

        mysqlPasswd = props.get(MYSQL_PASSWD);
        if (mysqlPasswd == null || mysqlPasswd.isEmpty()) {
            throw new ConnectException("mysqlSinkConnector should include 'mysql.passwd' setting.");
        }

        tableName = props.get(TABLE_NAME);
        if (tableName == null || tableName.isEmpty()) {
            throw new ConnectException("mysqlSinkConnector should include 'table.name' setting.");
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return mysqlSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();

        config.put(TOPIC_CONFIG, topic);
        config.put(MYSQL_URL, mysqlUrl);
        config.put(MYSQL_USER, mysqlUser);
        config.put(MYSQL_PASSWD, mysqlPasswd);
        config.put(TABLE_NAME, tableName);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
        //
    }
}
