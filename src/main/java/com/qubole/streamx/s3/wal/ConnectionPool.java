package com.qubole.streamx.s3.wal;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.util.HashMap;


public class ConnectionPool {
  private static final Logger log = LoggerFactory.getLogger(ConnectionPool.class);

  private static HashMap<String, ComboPooledDataSource> dataSources = new HashMap<>();
  public static ComboPooledDataSource getDatasource(String connectionUrl, String user, String password) {
    if(dataSources.containsKey(connectionUrl)) {
      return dataSources.get(connectionUrl);
    }
    else {
      try {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver"); //loads the jdbc driver
        cpds.setJdbcUrl(connectionUrl);
        cpds.setUser(user);
        cpds.setPassword(password);
        cpds.setMaxConnectionAge(18000);//5 hours
        return cpds;
      } catch (PropertyVetoException e) {
        log.error("Error while creating c3p0 ComboPooledDataSource" + e);
        throw new ConnectException(e);
      }
    }
  }

}
