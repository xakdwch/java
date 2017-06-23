package io.connector.mysql;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Objects;
import java.io.IOException;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;

public class dataConvertor {

    public static Schema convertSchema(String tableName, ResultSetMetaData metadata) throws SQLException {
        SchemaBuilder builder = SchemaBuilder.struct().name(tableName);

        for (int col = 1; col <= metadata.getColumnCount(); col++) {
            addFieldSchema(builder, metadata, col);
        }

        return builder.build();
    }

    public static void addFieldSchema(SchemaBuilder builder, ResultSetMetaData metadata, int col) throws SQLException {
        String fieldName = metadata.getColumnName(col);
        int sqlType = metadata.getColumnType(col);

        switch (sqlType) {
            case Types.BOOLEAN: {
                builder.field(fieldName, Schema.BOOLEAN_SCHEMA);
                break;
            }
           case Types.BIT: {
                builder.field(fieldName, Schema.INT8_SCHEMA);
                break;
            }
            case Types.TINYINT: {
                if (metadata.isSigned(col)) {
                    builder.field(fieldName, Schema.INT8_SCHEMA);
                }
                else {
                    builder.field(fieldName, Schema.INT16_SCHEMA);
                }
                break;
            }
            case Types.SMALLINT: {
                if (metadata.isSigned(col)) {
                    builder.field(fieldName, Schema.INT16_SCHEMA);
                }
                else {
                    builder.field(fieldName, Schema.INT32_SCHEMA);
                }
                break;
            }
            case Types.INTEGER: {
                if (metadata.isSigned(col)) {
                    builder.field(fieldName, Schema.INT32_SCHEMA);
                }
                else {
                    builder.field(fieldName, Schema.INT64_SCHEMA);
                }
                break;
            }
            case Types.BIGINT: {
                builder.field(fieldName, Schema.INT64_SCHEMA);
                break;
            }
            case Types.REAL: {
                builder.field(fieldName, Schema.INT32_SCHEMA);
                break;
            }
            case Types.FLOAT:
            case Types.DOUBLE: {
                builder.field(fieldName, Schema.FLOAT64_SCHEMA);
                break;
            }
            case Types.NUMERIC: {
                int precision = metadata.getPrecision(col);
                if (metadata.getScale(col) == 0 && precision < 19) {
                    Schema numericSchema;
                    if (precision > 9) {
                        numericSchema = Schema.INT64_SCHEMA;
                    }
                    else if (precision > 4) {
                        numericSchema = Schema.INT32_SCHEMA;
                    }
                    else if (precision > 2) {
                        numericSchema = Schema.INT16_SCHEMA;
                    }
                    else {
                        numericSchema = Schema.INT8_SCHEMA;
                    }
                    builder.field(fieldName, numericSchema);
                }
                break;
            }
            case Types.DECIMAL: {
                int scale = metadata.getScale(col);
                if (scale == -127) {
                    scale = 127;
                }
                SchemaBuilder fieldBuilder = Decimal.builder(scale);
                builder.field(fieldName, fieldBuilder.build());
                break;
            }
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.DATALINK:
            case Types.SQLXML: {
                builder.field(fieldName, Schema.STRING_SCHEMA);
                break;
            }
            case Types.BINARY:
            case Types.BLOB:
            case Types.VARBINARY:
            case Types.LONGVARBINARY: {
                builder.field(fieldName, Schema.BYTES_SCHEMA);
                break;
            }
            case Types.DATE: {
                SchemaBuilder fieldBuilder = Date.builder();
                builder.field(fieldName, fieldBuilder.build());
                break;
            }
            case Types.TIME: {
                SchemaBuilder fieldBuilder = Time.builder();
                builder.field(fieldName, fieldBuilder.build());
                break;
            }
            case Types.TIMESTAMP: {
                SchemaBuilder fieldBuilder = Timestamp.builder();
                builder.field(fieldName, fieldBuilder.build());
                break;
            }
            default: {
                System.out.println("Unsupport JDBC sql type " + sqlType);
                break;
            }
        }
    }

    public static Struct convertRecord(Schema schema, ResultSet resultset) throws SQLException {
        Struct struct = new Struct(schema);
        ResultSetMetaData metadata = resultset.getMetaData();

        for (int col = 1; col <= metadata.getColumnCount(); col++) {
            try {
                convertFieldValue(resultset, col, metadata.getColumnType(col), struct, metadata.getColumnName(col));
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return struct;
    }

    public static void convertFieldValue(ResultSet resultset, int col, int colType, Struct struct, String fieldName) throws SQLException, IOException {
        Object fieldValue = null;
        switch (colType) {
            case Types.NULL: {
                fieldValue = null;
                break;
            }
            case Types.BOOLEAN: {
                fieldValue = resultset.getBoolean(col);
                break;
            }
           case Types.BIT: {
                fieldValue = resultset.getByte(col);
                break;
            }
            case Types.TINYINT: {
                ResultSetMetaData metadata = resultset.getMetaData();
                if (metadata.isSigned(col)) {
                    fieldValue = resultset.getByte(col);
                }
                else {
                    fieldValue = resultset.getShort(col);
                }
                break;
            }
            case Types.SMALLINT: {
                ResultSetMetaData metadata = resultset.getMetaData();
                if (metadata.isSigned(col)) {
                    fieldValue = resultset.getShort(col);
                }
                else {
                    fieldValue = resultset.getInt(col);
                }
                break;
            }
            case Types.INTEGER: {
                ResultSetMetaData metadata = resultset.getMetaData();
                if (metadata.isSigned(col)) {
                    fieldValue = resultset.getInt(col);
                }
                else {
                    fieldValue = resultset.getLong(col);
                }
                break;
            }
            case Types.BIGINT: {
                fieldValue = resultset.getLong(col);
                break;
            }
            case Types.REAL: {
                fieldValue = resultset.getFloat(col);
                break;
            }
            case Types.FLOAT:
            case Types.DOUBLE: {
                fieldValue = resultset.getDouble(col);
                break;
            }
            case Types.NUMERIC: {
                ResultSetMetaData metadata = resultset.getMetaData();
                int precision = metadata.getPrecision(col);
                if (metadata.getScale(col) == 0 && precision < 19) {
                    if (precision > 9) {
                        fieldValue = resultset.getLong(col);
                    }
                    else if (precision > 4) {
                        fieldValue = resultset.getInt(col);
                    }
                    else if (precision > 2) {
                        fieldValue = resultset.getShort(col);
                    }
                    else {
                        fieldValue = resultset.getByte(col);
                    }
                }
                break;
            }
            case Types.DECIMAL: {
                fieldValue = resultset.getBigDecimal(col);
                break;
            }
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR: {
                fieldValue = resultset.getString(col);
                break;
            }
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR: {
                fieldValue = resultset.getNString(col);
                break;
            }
            case Types.CLOB:
            case Types.NCLOB: {
                Clob clob = (colType == Types.CLOB ? resultset.getClob(col) : resultset.getNClob(col));
                if (clob == null) {
                    fieldValue = null;
                } else {
                    if (clob.length() > Integer.MAX_VALUE) {
                        throw new IOException("Can't process BLOBs longer than Integer.MAX_VALUE");
                    }
                    fieldValue = clob.getSubString(1, (int) clob.length());
                    clob.free();
                }
                break;
            }
            case Types.DATALINK: {
                URL url = resultset.getURL(col);
                fieldValue = (url != null ? url.toString() : null);
                break;
            }
            case Types.SQLXML: {
                SQLXML xml = resultset.getSQLXML(col);
                fieldValue = (xml != null ? xml.toString() : null) ;
                break;
            }
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY: {
                fieldValue = resultset.getByte(col);
                break;
            }
            case Types.BLOB: {
                Blob blob = resultset.getBlob(col);
                if (blob == null) {
                    fieldValue = null;
                } else {
                    if (blob.length() > Integer.MAX_VALUE) {
                        throw new IOException("Can't process BLOBs longer than Integer.MAX_VALUE");
                    }
                    fieldValue = blob.getBytes(1, (int) blob.length());
                    blob.free();
                }
                break;
            }
            case Types.DATE: {
                fieldValue = resultset.getDate(col);
                break;
            }
            case Types.TIME: {
                fieldValue = resultset.getTime(col);
                break;
            }
            case Types.TIMESTAMP: {
                fieldValue = resultset.getTimestamp(col);
                break;
            }
            default: {
                System.out.println("Unsupport JDBC sql type " + colType);
                return;
            }
        }
        struct.put(fieldName, fieldValue);
    }

    public static void toSuitableFormat(StringBuilder builder, Object obj) {
        if (obj instanceof String) {
            builder.append("'");
            builder.append(obj);
            builder.append("'");
        }
        else if (obj instanceof java.util.Date) {
            builder.append("'");
            builder.append(obj);
            builder.append("'");
        }
        else {
            builder.append(obj);
        }
    }
}
