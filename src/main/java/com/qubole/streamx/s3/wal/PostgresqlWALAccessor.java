package com.qubole.streamx.s3.wal;

import com.qubole.streamx.s3.S3Util;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Postgresql WAL Accessor.
 */
public class PostgresqlWALAccessor extends AbstractDBWALAccessor {
  private static final Logger log = LoggerFactory.getLogger(PostgresqlWALAccessor.class);

  public PostgresqlWALAccessor(String connectionURL, String user, String password, String tableName) throws SQLException {
    super(connectionURL, user, password, tableName);
  }

  @Override
  public void createLeaseTable() throws SQLException {
    Statement statement = connection.createStatement();
    statement.setQueryTimeout(30);  // set timeout to 30 sec.

    log.info("Creating lease table");
    String sql = String.format("CREATE TABLE %s (\n" +
        " ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
        " id int DEFAULT NULL,\n" +
        " wal VARCHAR" +
        ") ", AbstractDBWALAccessor.LEASE_TABLE_NAME);
    createTableIfNotExists(AbstractDBWALAccessor.LEASE_TABLE_NAME, sql);

    log.info("Creating trigger on update, update timestamp field");
    sql = String.format("CREATE FUNCTION update_ts_column() RETURNS trigger\n" +
        "    LANGUAGE plpgsql\n" +
        "    AS $$\n" +
        "  BEGIN\n" +
        "    NEW.ts = NOW();\n" +
        "    RETURN NEW;\n" +
        "  END;\n" +
        "$$;");
    statement.executeUpdate(sql);

    try{
      sql = String.format("CREATE TRIGGER least_table_ts_modtime BEFORE UPDATE ON %s FOR EACH ROW EXECUTE PROCEDURE update_ts_column();", AbstractDBWALAccessor.LEASE_TABLE_NAME);
      statement.executeUpdate(sql);
    } catch(SQLException e){
      log.info("CREATE TRIGGER on update exception is ignored", e);
    }


  }

  @Override
  public void createWalTable(String tablePrefixName, TopicPartition topicPartition) throws SQLException {
    String topicName = topicPartition.topic();
    int partitionId = topicPartition.partition();

    String tableName = tablePrefixName + "_" + S3Util.cleanTopicNameForDBWal(topicName) + "_" + partitionId;

    String sql = String.format(
        "create table %s (id SERIAL, tempFiles VARCHAR, committedFiles VARCHAR, primary key (id))",
        tableName
    );
    createTableIfNotExists(tableName, sql);
  }
}
