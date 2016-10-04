package io.confluent.connect.hdfs.hive;

import io.confluent.connect.hdfs.errors.HiveMetaStoreException;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public interface HiveMetaStore {
    void addPartition(String database, String tableName, String path) throws HiveMetaStoreException;

    void dropPartition(String database, String tableName, String path) throws HiveMetaStoreException;

    void createDatabase(String database) throws HiveMetaStoreException;

    void dropDatabase(String name, boolean deleteData) throws HiveMetaStoreException;

    void createTable(Table table) throws HiveMetaStoreException;

    void alterTable(Table table) throws HiveMetaStoreException;

    void dropTable(String database, String tableName);

    boolean tableExists(String database, String tableName) throws HiveMetaStoreException;

    Table getTable(String database, String tableName) throws HiveMetaStoreException;

    List<String> listPartitions(String database, String tableName, short max) throws HiveMetaStoreException;

    List<String> getAllTables(String database) throws HiveMetaStoreException;

    List<String> getAllDatabases() throws HiveMetaStoreException;
}
