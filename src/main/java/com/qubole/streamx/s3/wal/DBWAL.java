/**
 * Copyright 2015 Qubole Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package com.qubole.streamx.s3.wal;

import com.mchange.v2.c3p0.*;
import com.qubole.streamx.s3.S3SinkConnectorConfig;
import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.hdfs.wal.WAL;
import org.apache.hadoop.util.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class DBWAL implements  WAL {

  private static final Logger log = LoggerFactory.getLogger(DBWAL.class);

  String tableName;
  Storage storage;
  int partitionId = -1;
  int id = ThreadLocalRandom.current().nextInt(1, 100000 + 1);
  HdfsSinkConnectorConfig config;
  String lease_table = "streamx_lease";

  ArrayList<String> tempFiles = new ArrayList<>();
  ArrayList<String> committedFiles = new ArrayList<>();

  ComboPooledDataSource cpds = null;

  public DBWAL(String logsDir, TopicPartition topicPartition, Storage storage, HdfsSinkConnectorConfig config) {
    this.storage = storage;
    this.config = config;
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    partitionId = topicPartition.partition();

    try {
      String name = config.getString(S3SinkConnectorConfig.NAME_CONFIG);
      tableName = name + "_" + topicPartition.topic() + "_" + partitionId;

      String connectionURL = config.getString(S3SinkConnectorConfig.DB_CONNECTION_URL_CONFIG);
      String user = config.getString(S3SinkConnectorConfig.DB_USER_CONFIG);
      String password = config.getString(S3SinkConnectorConfig.DB_PASSWORD_CONFIG);
      if(connectionURL.length()==0 || user.length()==0 || password.length()==0)
        throw new ConnectException("db.connection.url,db.user,db.password - all three properties must be specified");

      cpds = ConnectionPool.getDatasource(connectionURL, user, password);

      String sql = String.format("create table %s (id INT AUTO_INCREMENT, tempFiles VARCHAR(500), committedFiles VARCHAR(500), primary key (id))", tableName);
      createIfNotExists(tableName, sql);

      sql = String.format("CREATE TABLE `%s` (\n" +
          " `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
          " `id` int(11) DEFAULT NULL,\n" +
          " `wal` VARCHAR(500)" +
          ") ", lease_table);
      createIfNotExists("streamx_lease", sql);

     } catch (SQLException e) {
       log.error(e.toString());
       throw new ConnectException(e);
     }
  }

  private void createIfNotExists(String tableName, String sql) throws SQLException {
    try (Connection connection = cpds.getConnection()) {
      connection.setAutoCommit(false);
      Statement statement = connection.createStatement();
      statement.setQueryTimeout(30);  // set timeout to 30 sec.
      DatabaseMetaData dbm = connection.getMetaData();
      ResultSet tables = dbm.getTables(null, null, tableName, null);

      if (tables.next()) {
        // No op
      } else {
        log.info("Creating table " + sql);
        statement.executeUpdate(sql);
        connection.commit();
      }
    }
  }

  @Override
  public void acquireLease() throws ConnectException {

    long sleepIntervalMs = 1000L;
    long MAX_SLEEP_INTERVAL_MS = 90000L;
    while (sleepIntervalMs < MAX_SLEEP_INTERVAL_MS) {

      try (Connection connection = cpds.getConnection()) {
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();
        statement.setQueryTimeout(5);  // set timeout to 30 sec.
        String sql = String.format("select now() as currentTS,l1.* from %s as l1 where wal = '%s' for update", lease_table, tableName);

        ResultSet rs = statement.executeQuery(sql);
        if (!rs.next()) {
          sql = String.format("insert into %s(id,wal) values (%s,'%s')", lease_table, id, tableName);
          statement.executeUpdate(sql);
          connection.commit();
          return;
        }
        if (canAcquireLock(rs.getTimestamp("currentTS"), rs.getTimestamp("ts"), rs.getInt("id"))) {
          sql = String.format("update %s set id=%s,ts=now() where wal='%s'", lease_table, id, tableName);
          statement.executeUpdate(sql);
          connection.commit();
          return;
        }
        connection.commit();
      } catch (SQLException e) {
        log.error(e.toString());
        throw new ConnectException(e);
      }

      try {
        Thread.sleep(sleepIntervalMs);
      } catch (InterruptedException ie) {
        throw new ConnectException(ie);
      }
      sleepIntervalMs = sleepIntervalMs * 2;
      if (sleepIntervalMs >= MAX_SLEEP_INTERVAL_MS) {
        throw new ConnectException("Cannot acquire lease after timeout, will retry.");
      }
    }
  }

  private boolean canAcquireLock(Timestamp now, Timestamp recordTS, int id) throws SQLException{
    if ((now.getTime() - recordTS.getTime()) >= 30*1000) {
      log.debug("last update is more than a 30 seconds" + now + " " + recordTS);
      return true;
    }
    else {
      log.debug("last update within 30 seconds"+ now + " " + recordTS);
      // Check if this current thread holds the lock
      if(this.id == id)
        return true;
      else
        return false;
    }
  }

  @Override
  public void append(String tempFile, String committedFile) throws ConnectException {
    try {
      if (tempFile == WAL.beginMarker) {
        tempFiles.clear();
        committedFiles.clear();
      }
      else if (tempFile == WAL.endMarker) {
        String tempFilesCommaSeparated = StringUtils.join(",",tempFiles);
        String committedFilesCommaSeparated = StringUtils.join(",",committedFiles);

        acquireLease();
        try (Connection connection = cpds.getConnection()) {
          connection.setAutoCommit(false);

          Statement statement = connection.createStatement();
          statement.setQueryTimeout(30);  // set timeout to 30 sec.

          String sql = String.format("insert into %s (tempFiles,committedFiles) values ('%s','%s')", tableName, tempFilesCommaSeparated, committedFilesCommaSeparated);
          log.info("committing " + sql);
          statement.executeUpdate(sql);
          connection.commit();
        }
      }
      else {
        tempFiles.add(tempFile);
        committedFiles.add(committedFile);
      }
    } catch (SQLException e){
      log.error(e.toString());
      throw new ConnectException(e);
    }
  }

  @Override
  public void apply() throws ConnectException {
    try {
      acquireLease();
      try (Connection connection = cpds.getConnection()) {
        connection.setAutoCommit(false);

        Statement statement = connection.createStatement();
        statement.setQueryTimeout(30);  // set timeout to 30 sec.

        String sql = String.format("select * from %s order by id desc limit 1", tableName);
        log.info("Reading wal " + sql);
        ResultSet rs = statement.executeQuery(sql);

        while (rs.next()) {
          String tempFiles = rs.getString("tempFiles");
          String committedFiles = rs.getString("committedFiles");
          String tempFile[] = tempFiles.split(",");
          String committedFile[] = committedFiles.split(",");
          //TODO : check if all tempFiles are there.
          try {
            for (int k = 0; k < tempFile.length; k++) {
              storage.commit(tempFile[k], committedFile[k]);
              log.info("Recovering file " + tempFile[k] + " " + committedFile[k]);
            }
          } catch (IOException e) {
            e.printStackTrace();
            throw new ConnectException(e);
          }
        }
      }
    } catch (SQLException e){
      log.error(e.toString());
      throw new ConnectException(e);
    }
  }

  @Override
  public void truncate() throws ConnectException {
    try {
      try (Connection connection = cpds.getConnection()) {
        connection.setAutoCommit(false);

        Statement statement = connection.createStatement();
        statement.setQueryTimeout(30);  // set timeout to 30 sec.
        String sql = String.format("select * from %s order by id desc limit 1", tableName);
        ResultSet rs = statement.executeQuery(sql);
        if(rs.next()) {
          String id = rs.getString("id");
          sql = String.format("delete from %s where id < %s", tableName, id);
          log.info("truncating table " + sql);
          statement.executeUpdate(sql);
          connection.commit();
        }
      }
    } catch (SQLException e){
      log.error(e.toString());
      throw new ConnectException(e);
    }
  }

  @Override
  public void close() throws ConnectException {
  }

  @Override
  public String getLogFile() {
    return tableName;
  }

  @Override
  public long readOffsetFromWAL() {
    ResultSet rs = null;
    try (Connection connection = cpds.getConnection()){
      connection.setAutoCommit(false);
      Statement statement = connection.createStatement();
      statement.setQueryTimeout(30);  // set timeout to 30 sec.
      String sql = String.format("select * from %s order by id desc limit 1", tableName);
      rs = statement.executeQuery(sql);
      rs.next();
      String committedFiles[] = rs.getString("committedFiles").split(",");
      long maxOffset = -1;
      for(String committedFile: committedFiles) {
        long offset = FileUtils.extractOffset(committedFile);
        if (offset > maxOffset)
          maxOffset = offset;
      }
      log.info("Offset from WAL " + (maxOffset + 1) + " for topic partition id " + partitionId);
      return maxOffset + 1;
    } catch (SQLException e) {
      log.error("Exception while reading offset from WAL " + e.toString());
    }
    return -1L;
  }

}

