package com.qubole.streamx.s3.wal;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Abstract class for accessing DB Wal.
 */
public abstract class AbstractDBWALAccessor implements DBWALAccessor{
  private static final Logger log = LoggerFactory.getLogger(AbstractDBWALAccessor.class);
  public static final String LEASE_TABLE_NAME = "streamx_lease";

  /**
   * Factory class for initiating {@link AbstractDBWALAccessor}
   */
  private static class DBWALAccessorFactory{
    private static final Logger log = LoggerFactory.getLogger(DBWALAccessorFactory.class);
    private static AbstractDBWALAccessor getInstance(String connectionUrl, String user, String password, String tableName) throws SQLException{
      String[] splitted = connectionUrl.split(":");

      // check if the format is wrong
      if (splitted.length < 3){
        throw new IllegalArgumentException("incorrect database connectionUrl for " + connectionUrl);
      }

      String jdbcType = splitted[1].toLowerCase(); //lowercased to prevent mistakes
      log.info("Initializing DBWAL with jdbcType: " + jdbcType);
      // still using the old way to load jdbc class
      try{
        if ("postgresql".equals(jdbcType)){
          Class.forName("org.postgresql.Driver");
          return new PostgresqlWALAccessor(connectionUrl, user, password, tableName);
        } else if ("mysql".equals(jdbcType)){
          Class.forName("com.mysql.jdbc.Driver");
          return new MysqlWALAccessor(connectionUrl, user, password, tableName);
        }
      }catch (ClassNotFoundException e) {
        log.error("cannot find suitable class for: {}", jdbcType, e);
      }
      return null;
    }
  }

  /**
   * Get the correct instance of {@link AbstractDBWALAccessor}
   * @param connectionUrl
   * @param user
   * @param password
   * @return
   */
  public static AbstractDBWALAccessor getInstance(String connectionUrl, String user, String password,  String tableName) throws SQLException{
    return DBWALAccessorFactory.getInstance(connectionUrl, user, password, tableName);
  }

  protected Connection connection;
  protected final String _user;
  protected final String _connectionURL;
  protected final String tableName;

  public AbstractDBWALAccessor(String connectionURL, String user, String password, String tableName) throws SQLException{
    _connectionURL = connectionURL;
    _user = user;
    this.tableName = tableName;
    connection = DriverManager.getConnection(_connectionURL, _user, password);
    connection.setAutoCommit(false);
  }

  public String getUser() {
    return _user;
  }

  public String getConnectionURL() {
    return _connectionURL;
  }

  @Override
  public void createTableIfNotExists(String destTableName, String sql) throws SQLException {
    Statement statement = connection.createStatement();
    statement.setQueryTimeout(30);  // set timeout to 30 sec.
    DatabaseMetaData dbm = connection.getMetaData();
    ResultSet tables = dbm.getTables(null, null, destTableName, null);
    if (tables.next()) {
      // No op
      log.info("Table " + destTableName + " has already exists");
    }
    else {
      log.info("Creating table "+ sql);
      statement.executeUpdate(sql);
      connection.commit();
    }
  }

  @Override
  public void createLeaseTable() throws SQLException {
    String sql = String.format("CREATE TABLE `%s` (\n" +
        " `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
        " `id` int(11) DEFAULT NULL,\n" +
        " `wal` VARCHAR(500)" +
        ") ", AbstractDBWALAccessor.LEASE_TABLE_NAME);
    createTableIfNotExists(AbstractDBWALAccessor.LEASE_TABLE_NAME, sql);
  }

  @Override
  public void createWalTable(String tablePrefixName, TopicPartition topicPartition) throws SQLException {
    String sql = String.format(
        "create table %s (id INT AUTO_INCREMENT, tempFiles VARCHAR(500), committedFiles VARCHAR(500), primary key (id))",
        tableName
    );
    createTableIfNotExists(tableName, sql);
  }

  @Override
  public void insertCommitedFile(String tempFile, String commitedFile) throws SQLException {
    Statement statement = connection.createStatement();
    statement.setQueryTimeout(30);  // set timeout to 30 sec.

    String sql = String.format("insert into %s (tempFiles,committedFiles) values ('%s','%s')", tableName, tempFile, commitedFile);
    log.info("committing " + sql);
    statement.executeUpdate(sql);
    connection.commit();
  }

  @Override
  public ResultSet getLastResultSetFromWalTable() throws SQLException{
    Statement statement = connection.createStatement();
    statement.setQueryTimeout(30);  // set timeout to 30 sec.

    String sql = String.format("select * from %s order by id desc limit 1", tableName);
    log.info("Reading wal " + sql);
    return statement.executeQuery(sql);
  }

  @Override
  public ResultSet getLastNResultsetFromWalTable(int n) throws SQLException{
    Statement statement = connection.createStatement();
    statement.setQueryTimeout(30);  // set timeout to 30 sec.

    String sql = String.format("select * from %s order by id desc limit %s", tableName, n);
    log.info("Reading wal " + sql);
    return statement.executeQuery(sql);
  }

  @Override
  public void truncateTableLessThanId(String id) throws SQLException{
    Statement statement = connection.createStatement();
    String sql = String.format("delete from %s where id < %s", tableName, id);

    log.info("truncating table " + sql);

    statement.executeUpdate(sql);
    connection.commit();
  }


  @Override
  public ResultSet getLeaseResultSetLockedRow() throws SQLException{
    Statement statement = connection.createStatement();
    statement.setQueryTimeout(5);  // set timeout to 30 sec.
    String sql = String.format("select now() as currentTS,l1.* from %s as l1 where wal = '%s' for update", LEASE_TABLE_NAME, tableName);

    return statement.executeQuery(sql);
  }

  @Override
  public void insertLeaseTableRow(int threadId) throws SQLException{
    Statement statement = connection.createStatement();
    statement.setQueryTimeout(5);  // set timeout to 30 sec.

    String sql = String.format("insert into %s(id,wal) values (%s,'%s')", LEASE_TABLE_NAME, threadId, tableName);
    statement.executeUpdate(sql);
    connection.commit();
  }

  @Override
  public void updateLeaseTableRow(int threadId) throws SQLException{
    Statement statement = connection.createStatement();
    statement.setQueryTimeout(5);  // set timeout to 30 sec.

    String sql = String.format("update %s set id=%s,ts=now() where wal='%s'", LEASE_TABLE_NAME, threadId, tableName);
    statement.executeUpdate(sql);
    connection.commit();
  }
}
