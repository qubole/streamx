package com.qubole.streamx.s3.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * DBWAL Accessor for Mysql.
 */
public class MysqlWALAccessor extends AbstractDBWALAccessor {
  private static final Logger log = LoggerFactory.getLogger(MysqlWALAccessor.class);

  public MysqlWALAccessor(String connectionURL, String user, String password, String tableName) throws SQLException {
    super(connectionURL, user, password, tableName);
  }



}
