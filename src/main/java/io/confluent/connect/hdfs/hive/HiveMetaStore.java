/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.hdfs.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.errors.HiveMetaStoreException;

public class HiveMetaStore {

  private static final Logger log = LoggerFactory.getLogger(HiveMetaStore.class);
  private final IMetaStoreClient client;

  public HiveMetaStore(Configuration conf, HdfsSinkConnectorConfig connectorConfig) throws HiveMetaStoreException {
    HiveConf hiveConf = new HiveConf(conf, HiveConf.class);
    String hiveConfDir = connectorConfig.getString(HdfsSinkConnectorConfig.HIVE_CONF_DIR_CONFIG);
    String hiveMetaStoreURIs = connectorConfig.getString(HdfsSinkConnectorConfig.HIVE_METASTORE_URIS_CONFIG);
    if (!hiveConfDir.equals("")) {
      String hiveSitePath = hiveConfDir + "/hive-site.xml";
      File hiveSite = new File(hiveSitePath);
      if (!hiveSite.exists()) {
        log.warn("hive-site.xml does not exist in provided Hive configuration directory {}.", hiveConf);
      }
      hiveConf.addResource(new Path(hiveSitePath));
    }
    hiveConf.set("hive.metastore.uris", hiveMetaStoreURIs);
    try {
      client = HCatUtil.getHiveMetastoreClient(hiveConf);
    } catch (IOException | MetaException e) {
      throw new HiveMetaStoreException(e);
    }
  }

  private interface ClientAction<R> {
    R call() throws TException;
  }

  private <R> R doWithRetry(ClientAction<R> action) throws TException {
    try {
      return action.call();
    } catch (TException e) {
      try {
          client.reconnect();
      } catch (MetaException swallowedException) {
        // reconnect failed, throw the original exception
        log.warn("Cannot connect to Hive meta store: {}", swallowedException.getMessage());
        throw e;
      }
      return action.call();
    }
  }

  public void addPartition(final String database, final String tableName, final String path) throws HiveMetaStoreException {
    ClientAction<Void> addPartition = new ClientAction<Void>() {
      @Override
      public Void call() throws TException {
        // purposely don't check if the partition already exists because
        // getPartition(db, table, path) will throw an exception to indicate the
        // partition doesn't exist also. this way, it's only one call.
        client.appendPartition(database, tableName, path);
        return null;
      }
    };

    try {
      doWithRetry(addPartition);
    } catch (AlreadyExistsException e) {
      // this is okay
    } catch (InvalidObjectException e) {
      throw new HiveMetaStoreException("Invalid partition for " + database + "." + tableName + ": " + path, e);
    } catch (MetaException e) {
      throw new HiveMetaStoreException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new HiveMetaStoreException("Exception communicating with the Hive MetaStore", e);
    }
  }

  public void dropPartition(final String database, final String tableName, final String path) throws HiveMetaStoreException {
    ClientAction<Void> dropPartition = new ClientAction<Void>() {
      @Override
      public Void call() throws TException {
        client.dropPartition(database, tableName, path, false);
        return null;
      }
    };

    try {
      doWithRetry(dropPartition);
    } catch (NoSuchObjectException e) {
      // this is okay
    } catch (InvalidObjectException e) {
      throw new HiveMetaStoreException("Invalid partition for " + database + "." + tableName + ": " + path, e);
    } catch (MetaException e) {
      throw new HiveMetaStoreException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new HiveMetaStoreException("Exception communicating with the Hive MetaStore", e);
    }
  }


  public void createDatabase(final String database) throws HiveMetaStoreException {
    ClientAction<Void> create = new ClientAction<Void>() {
      @Override
      public Void call() throws TException {
        client.createDatabase(new Database(database, "Database created by Kafka Connect", null, null));
        return null;
      }
    };

    try {
      doWithRetry(create);
    } catch (AlreadyExistsException e) {
      log.warn("Hive database already exists: {}", database);
    } catch (InvalidObjectException e) {
      throw new HiveMetaStoreException("Invalid database: " + database, e);
    } catch (MetaException e) {
      throw new HiveMetaStoreException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new HiveMetaStoreException("Exception communicating with the Hive MetaStore", e);
    }
  }


  public void dropDatabase(final String name, final boolean deleteData) throws HiveMetaStoreException {
    ClientAction<Void> drop = new ClientAction<Void>() {
      @Override
      public Void call() throws TException {
        client.dropDatabase(name, deleteData, true);
        return null;
      }
    };

    try {
      doWithRetry(drop);
    } catch (NoSuchObjectException e) {
      // this is okey
    } catch (MetaException e) {
      throw new HiveMetaStoreException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new HiveMetaStoreException("Exception communicating with the Hive MetaStore", e);
    }
  }

  public void createTable(final Table table) throws HiveMetaStoreException {
    ClientAction<Void> create = new ClientAction<Void>() {
      @Override
      public Void call() throws TException {
        client.createTable(table.getTTable());
        return null;
      }
    };

    createDatabase(table.getDbName());

    try {
      doWithRetry(create);
    } catch (NoSuchObjectException e) {
      throw new HiveMetaStoreException("Hive table not found: " + table.getDbName() + "." + table.getTableName());
    } catch (AlreadyExistsException e) {
      // this is okey
      log.warn("Hive table already exists: {}.{}", table.getDbName(), table.getTableName());
    } catch (InvalidObjectException e) {
      throw new HiveMetaStoreException("Invalid table", e);
    } catch (MetaException e) {
      throw new HiveMetaStoreException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new HiveMetaStoreException("Exception communicating with the Hive MetaStore", e);
    }
  }

  public void alterTable(final Table table) throws HiveMetaStoreException {
    ClientAction<Void> alter = new ClientAction<Void>() {
      @Override
      public Void call() throws TException {
        client.alter_table(table.getDbName(), table.getTableName(), table.getTTable());
        return null;
      }
    };

    try {
      doWithRetry(alter);
    } catch (NoSuchObjectException e) {
      throw new HiveMetaStoreException("Hive table not found: " + table.getDbName() + "." + table.getTableName());
    } catch (InvalidObjectException e) {
      throw new HiveMetaStoreException("Invalid table", e);
    } catch (InvalidOperationException e) {
      throw new HiveMetaStoreException("Invalid table change", e);
    } catch (MetaException e) {
      throw new HiveMetaStoreException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new HiveMetaStoreException("Exception communicating with the Hive MetaStore", e);
    }
  }

  public void dropTable(final String database, final String tableName) {
    ClientAction<Void> drop = new ClientAction<Void>() {
      @Override
      public Void call() throws TException {
        client.dropTable(database, tableName, false, true);
        return null;
      }
    };

    try {
      doWithRetry(drop);
    } catch (NoSuchObjectException e) {
      // this is okay
    } catch (MetaException e) {
      throw new HiveMetaStoreException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new HiveMetaStoreException("Exception communicating with the Hive MetaStore", e);
    }
  }

  public boolean tableExists(final String database, final String tableName) throws HiveMetaStoreException {
    ClientAction<Boolean> exists = new ClientAction<Boolean>() {
      @Override
      public Boolean call() throws TException {
        return client.tableExists(database, tableName);
      }
    };
    try {
      return doWithRetry(exists);
    } catch (UnknownDBException e) {
      return false;
    } catch (MetaException e) {
      throw new HiveMetaStoreException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new HiveMetaStoreException("Exception communicating with the Hive MetaStore", e);
    }
  }

  public Table getTable(final String database, final String tableName) throws HiveMetaStoreException {
    ClientAction<Table> getTable = new ClientAction<Table>() {
      @Override
      public Table call() throws TException {
        return new Table(client.getTable(database, tableName));
      }
    };

    Table table;
    try {
      table = doWithRetry(getTable);
    } catch (NoSuchObjectException e) {
      throw new HiveMetaStoreException("Hive table not found: " + database + "." + tableName);
    } catch (MetaException e) {
      throw new HiveMetaStoreException("Hive table lookup exception", e);
    } catch (TException e) {
      throw new HiveMetaStoreException("Exception communicating with the Hive MetaStore", e);
    }

    if (table == null) {
      throw new HiveMetaStoreException("Could not find info for table: " + tableName);
    }
    return table;
  }

  public List<String> listPartitions(final String database, final String tableName, final short max) throws HiveMetaStoreException {
    ClientAction<List<String>> listPartitions = new ClientAction<List<String>>() {
      @Override
      public List<String> call() throws TException {
        List<Partition> partitions = client.listPartitions(database, tableName, max);
        List<String> paths = new ArrayList<>();
        for (Partition partition : partitions) {
          paths.add(partition.getSd().getLocation());
        }
        return paths;
      }
    };

    try {
      return doWithRetry(listPartitions);
    } catch (NoSuchObjectException e) {
      return new ArrayList<>();
    } catch (MetaException e) {
      throw new HiveMetaStoreException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new HiveMetaStoreException("Exception communicating with the Hive MetaStore", e);
    }
  }

  public List<String> getAllTables(final String database) throws HiveMetaStoreException {
    ClientAction<List<String>> getAllTables = new ClientAction<List<String>>() {
      @Override
      public List<String> call() throws TException {
        return client.getAllTables(database);
      }
    };

    try {
      return doWithRetry(getAllTables);
    } catch (NoSuchObjectException e) {
      return new ArrayList<>();
    } catch (MetaException e) {
      throw new HiveMetaStoreException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new HiveMetaStoreException("Exception communicating with the Hive MetaStore", e);
    }
  }

  public List<String> getAllDatabases() throws HiveMetaStoreException {
    ClientAction<List<String>> create =
        new ClientAction<List<String>>() {
          @Override
          public List<String> call() throws TException {
            return client.getAllDatabases();
          }
        };

    try {
      return doWithRetry(create);
    } catch (NoSuchObjectException e) {
      return new ArrayList<>();
    } catch (MetaException e) {
      throw new HiveMetaStoreException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new HiveMetaStoreException("Exception communicating with the Hive MetaStore", e);
    }
  }
}
