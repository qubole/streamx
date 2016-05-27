package com.qubole.streamx.hive;

import java.util.ArrayList;
import java.util.List;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.errors.HiveMetaStoreException;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.HdfsSinkConnector;
import io.confluent.connect.hdfs.hive.HiveMetaStore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.SerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.concurrent.Future;
import java.util.Map;
import java.util.HashMap;
import java.lang.Double;

public class HiveMetaStoreRESTImpl implements HiveMetaStore {

    private final JsonRestClient client;
    private final String commandEndpoint = "https://api.qubole.com/api/v1.2/commands";
    private static final Logger log = LoggerFactory.getLogger(HiveMetaStore.class);
    private static final int THREAD_SLEEP_TIME = 1000;

    public HiveMetaStoreRESTImpl(Configuration conf, HdfsSinkConnectorConfig connectorConfig) throws HiveMetaStoreException {
        client = new JsonRestClient(connectorConfig.getString(HdfsSinkConnectorConfig.HIVE_CONF_REST_API_KEY));
        log.info("Hive Metastore REST Implementation started");
    }

    // Generic Query Function
    private String hiveQuery(String query) throws HiveMetaStoreException {
        try {
            log.info("Hive REST Query: " + query);

            Map<String, String> queryMap = new HashMap<String, String>();
            queryMap.put("query", query);


            // POST request to endpoint to queue command
            Map result = client.postRequest(commandEndpoint, queryMap);

            // Get command ID from results: used later to check if the command completed successfully
            String commandId = String.format("%.0f", (Double)result.get("id"));

            boolean queryComplete = waitForCompletion(commandId, 60);

            // Timeout
            if(!queryComplete) {
                throw new Exception("Hive REST Endpoint Timeout");
            }

            log.info("Query Complete");

            // Get query results
            return getResults(commandId);
        } catch (Exception e) {
            throw new HiveMetaStoreException("Query Failed: " + query, e);
        }
    }

    // Wait for completion of command (if timeout == -1) or timeout and return
    private boolean waitForCompletion(String commandId, Integer timeout) throws HiveMetaStoreException {
        try {
            boolean queryComplete = false;
            int timeLeft = timeout;

            // Wait for query completion
            while (!queryComplete && (timeout < 0 || timeLeft > 0)) {
                String queryStatus = (String) client.getRequest(commandEndpoint + "/" + commandId).get("status");
                queryComplete = queryStatus.equals("done");
                Thread.sleep(THREAD_SLEEP_TIME);
                timeLeft--;
            }

            return queryComplete;
        } catch (IOException | InterruptedException e) {
            throw new HiveMetaStoreException("Request to REST Endpoint failed: ", e);
        }
    }

    private String getResults(String commandId) throws HiveMetaStoreException{
        try {
            String results = (String) client.getRequest(commandEndpoint + "/" + commandId + "/results").get("results");

            // Once query is done, log results
            log.info("Hive Query Result: {}", results);
            return results;
        } catch (IOException e) {
            throw new HiveMetaStoreException("GET request to REST Endpoint failed: ", e);
        }
    }

    private List<String> parseListResults(String queryResult) {
        return Arrays.asList(queryResult.split("[\r\n]+"));
    }

    private String parsePathToPartition(String path) {
        String[] partitionFields = path.split("/");
        StringBuilder stringBuilder = new StringBuilder();
        String delim = "";

        for(String partitionField: partitionFields) {
            stringBuilder.append(delim);
            stringBuilder.append(partitionField);
            delim = ", ";
        }

        return stringBuilder.toString();
    }

    // Note: Alter command does not allow db.table semantics so call USE db;
    public void addPartition(String database, String tableName, String path) throws HiveMetaStoreException {
        String queryResult = hiveQuery("USE " + database + "; ALTER TABLE " + tableName + " ADD IF NOT EXISTS PARTITION (" + parsePathToPartition(path) + ")");
        log.debug("Query Successful: {}", queryResult);
    }

    public void dropPartition(String database, String tableName, String path) throws HiveMetaStoreException {
        String queryResult = hiveQuery("USE " + database + "; ALTER TABLE " + tableName + " DROP IF EXISTS PARTITION " + path);
        log.debug("Query Successful: {}", queryResult);
    }

    public void createDatabase(String database) throws HiveMetaStoreException {
        String queryResult = hiveQuery("CREATE DATABASE IF NOT EXISTS" + database);
        log.debug("Query Successful: {}", queryResult);
    }

    public void dropDatabase(String name, boolean deleteData) {
        String queryResult = hiveQuery("DROP IF EXISTS " + name);
        log.debug("Query Successful: {}", queryResult);
    }


    public void createTable(Table table) {
        StringBuilder stringBuilder = new StringBuilder();
        String delim = ", ";
        stringBuilder.append("CREATE EXTERNAL TABLE IF NOT EXISTS " + table.getDbName() + "." + table.getTableName() + " (");

        // Iterate over column types, names and add to query
        StorageDescriptor sd = table.getSd();
        for(Iterator<FieldSchema> iterator = sd.getColsIterator(); iterator.hasNext(); stringBuilder.append(delim)) {
            FieldSchema fs = iterator.next();
            stringBuilder.append(fs.getName() + " " + fs.getType());
            if(fs.getComment() != null)
                stringBuilder.append(" COMMENT '" + fs.getComment() + "'");
            if(!iterator.hasNext())
                delim = ") ";
        }

        delim = ", ";
        // Get partition by columns
        List<FieldSchema> partCols = table.getPartCols();
        if(partCols.size() > 0) {
            stringBuilder.append(" PARTITIONED BY (");
            for (ListIterator<FieldSchema> iterator = partCols.listIterator(); iterator.hasNext(); stringBuilder.append(delim)) {
                FieldSchema fs = iterator.next();
                stringBuilder.append(fs.getName() + " " + fs.getType());
                if(fs.getComment() !=  "")
                    stringBuilder.append(" COMMENT '" + fs.getComment() + "'");
                if(!iterator.hasNext())
                    delim = ") ";
            }
        }
        // Set SerDe
        String serializationLib = table.getSerializationLib();

        // Construct SerDe parameters
        stringBuilder.append(" ROW FORMAT SERDE '" + serializationLib + "'");

        // Store as input/output format
        stringBuilder.append(" STORED AS ");

        // Set input format
        String inputFormatClass = sd.getInputFormat();
        stringBuilder.append(" INPUTFORMAT " + " '" + inputFormatClass + "' ");

        // Set output format
        String outputFormatClass = sd.getOutputFormat();
        stringBuilder.append(" OUTPUTFORMAT " + " '" + outputFormatClass + "' ");

        String queryResult = hiveQuery(stringBuilder.toString());
        log.debug("Query Successful: {}", queryResult);
    }

    public void alterTable(Table table) {
        // AvroHiveUtil: sets avro.schema.literal
        String avroSchemaLiteral = table.getParameters().get("avro.schema.literal");

        if(avroSchemaLiteral != null) {
            String queryResult = hiveQuery("USE " + table.getDbName() + "; ALTER TABLE " + table.getTableName() + " SET TBLPROPERTIES ('avro.schema.literal' = '" + avroSchemaLiteral + "')");
            log.debug("Query Successful: {}", queryResult);
        }
    }


    public void dropTable(String database, String tableName) {
        String queryResult = hiveQuery("DROP IF EXISTS TABLE " + database + "." + tableName);
        log.debug("Query Successful: {}", queryResult);
    }

    public boolean tableExists(String database, String tableName) {
        List<String> listTables = getAllTables(database);
        for (String s : listTables) {
            if (s.equals(tableName))
                return true;
        }
        return false;
    }

    // Return a new table with just the database and tablename set; used for alterTable diff
    public Table getTable(String database, String tableName) {
        Table table = new Table(database, tableName);
        return table;
    }

    public List<String> listPartitions(String database, String tableName, short max) {
        return parseListResults(hiveQuery("SHOW PARTITIONS "+ database + "." + tableName));
    }

    public List<String> getAllTables(String database) {
        return parseListResults(hiveQuery("USE "+ database + "; SHOW TABLES"));
    }

    public List<String> getAllDatabases() {
        return parseListResults(hiveQuery("SHOW DATABASES"));
    }

}

