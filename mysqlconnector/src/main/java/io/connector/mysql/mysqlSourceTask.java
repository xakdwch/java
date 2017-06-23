package io.connector.mysql;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.SystemTime;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;

public class mysqlSourceTask extends SourceTask {
    private Time time;
    private String batchSize;
    private jdbcHandle db;
    private linkedListQueue<jdbcQuerier> tableQueue;
    private jdbcQuerier querier;
    private AtomicBoolean stop;

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        String topic = props.get(mysqlSourceConnector.TOPIC_CONFIG);
        String mysqlUrl = props.get(mysqlSourceConnector.MYSQL_URL);
        String mysqlUser = props.get(mysqlSourceConnector.MYSQL_USER);
        String mysqlPasswd = props.get(mysqlSourceConnector.MYSQL_PASSWD);

        time = new SystemTime();
        batchSize = props.get(mysqlSourceConnector.BATCH_SIZE);
        db = new jdbcHandle(mysqlUrl, mysqlUser, mysqlPasswd);

        String table = props.get(mysqlSourceConnector.TABLE_NAME);
        Map<String, Object> offset = null;
        offset = context.offsetStorageReader().offset(Collections.singletonMap("table", table));
        querier = new jdbcQuerier(topic, table, offset);

        stop = new AtomicBoolean(false);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException{
        while (!stop.get()) {
            querier.startQuery(db.getDBConnection());

            final List<SourceRecord> records = new ArrayList<>();
            try {
                boolean hasNext = true;
                while (records.size() < Integer.parseInt(batchSize) && (hasNext = querier.hasNext())) {
                    records.add(querier.extractRecord());
                }

                if (!hasNext) {
                    querier.reset();
                    time.sleep(1000);
                }

                if (records.isEmpty()) {
                    continue;
                }

                return records;
            }
            catch (Exception e) {
                querier.reset();
                return null;
            }
        }

        return null;
    }

    @Override
    public void stop() {
        if (stop != null) {
            stop.set(true);
        }

        if (db != null) {
            db.closeConnection();
        }
    }
}
