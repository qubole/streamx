package com.qubole.streamx.s3.wal;

import org.apache.kafka.common.TopicPartition;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * DBWalAccessor
 */
public interface DBWALAccessor {
  void createTableIfNotExists(String tableName, String sql) throws SQLException;
  void createLeaseTable() throws SQLException;
  void createWalTable(String tablePrefixName, TopicPartition topicPartition) throws SQLException;
  void insertCommitedFile(String tempFile, String commitedFile) throws SQLException;
  ResultSet getLastResultSetFromWalTable() throws SQLException;
  ResultSet getLastNResultsetFromWalTable(int n) throws SQLException;
  void truncateTableLessThanId(String id) throws SQLException;
  ResultSet getLeaseResultSetLockedRow() throws SQLException;
  void insertLeaseTableRow(int threadId) throws SQLException;
  void updateLeaseTableRow(int threadId) throws SQLException;
}
