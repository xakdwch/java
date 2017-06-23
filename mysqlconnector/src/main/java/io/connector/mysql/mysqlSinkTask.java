package io.connector.mysql;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Field;

import java.util.Map;
import java.util.List;
import java.util.Collection;

public class mysqlSinkTask extends SinkTask {
    private String topic;
    private String mysqlUrl;
    private String mysqlUser;
    private String mysqlPasswd;
    private String tableName;
    private jdbcHandle db;

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(mysqlSinkConnector.TOPIC_CONFIG);
        mysqlUrl = props.get(mysqlSinkConnector.MYSQL_URL);
        mysqlUser = props.get(mysqlSinkConnector.MYSQL_USER);
        mysqlPasswd = props.get(mysqlSinkConnector.MYSQL_PASSWD);
        tableName = props.get(mysqlSinkConnector.TABLE_NAME);

        db = new jdbcHandle(mysqlUrl, mysqlUser, mysqlPasswd);
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        if (sinkRecords.isEmpty()) {
            System.out.println("records is empty");
            return;
        }

        

        StringBuilder builder = new StringBuilder();
        for (SinkRecord record : sinkRecords) {
            builder.append("INSERT INTO ");
            builder.append(tableName);
            builder.append(" VALUES(");
            List<Field> fields = record.valueSchema().fields();
            boolean first = true;
            for (Field field : fields) {
                if (first) {
                    dataConvertor.toSuitableFormat(builder, ((Struct)record.value()).get(field));
                    first = false;
                }
                else {
                    builder.append(",");
                    dataConvertor.toSuitableFormat(builder, ((Struct)record.value()).get(field));
                }
            }
            builder.append(")");
            System.out.println(builder.toString());
            builder.setLength(0);
        }

        
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        //
    }

    @Override
    public void stop() {
        if (db != null) {
            db.closeConnection();
        }
    }
}
