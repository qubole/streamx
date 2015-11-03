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

package io.confluent.copycat.hdfs.hive;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import io.confluent.copycat.hdfs.HdfsSinkConnectorConfig;

/**
 * Utility to create an external table for an directory with provided schema.
 */
public class HiveExec {

  public static final Log log = LogFactory.getLog(HiveExec.class);

  private Configuration conf;
  private HiveConf hiveConf;
  private CliDriver cliDriver;
  private final HiveQLWriter hiveQLWriter;
  private static final String HIVE_SASL_ENABLED = "hive.metastore.sasl.enabled";

  /**
   * HiveExec constructor
   * @param conf Hadoop configuration
   * @param config HDFS Connector configuration
   */
  public HiveExec(Configuration conf, HdfsSinkConnectorConfig config) {
    this.conf = conf;
    String topicsDir = config.getString(HdfsSinkConnectorConfig.TOPIC_DIR_CONFIG);
    String url = config.getString(HdfsSinkConnectorConfig.HDFS_URL_CONFIG);
    hiveConf = new HiveConf();
    String hiveConfDir = config.getString(HdfsSinkConnectorConfig.HIVE_CONF_DIR_CONFIG);
    hiveConf.addResource(new Path(hiveConfDir, "hive-site.xml"));
    SessionState.start(new CliSessionState(hiveConf));
    cliDriver = new CliDriver();
    hiveQLWriter = new HiveQLWriter(url, topicsDir);
  }

  /**
   * Create an Avro external table
   * @param tableName the name of the table to create in Hive.
   * @param avroSchema the schema of the Avro files in HDFS.
   */
  public void createAvroTable(String tableName, Schema avroSchema) throws IOException {
    // generate the HiveQL statements to run
    String createAvroTableStmt = hiveQLWriter.createAvroTableStmt(tableName, avroSchema) + ";\n";
    executeQuery(createAvroTableStmt);
    log.info("Create Avro table complete.");
  }

  /**
   * Alter the schema of an Avro external table
   * @param tableName the name of the table to create in Hive.
   * @param avroSchema the schema of the Avro files in HDFS.
   */
  public void alterSchema(String tableName, Schema avroSchema) throws IOException {
    String alterSchemaStmt = hiveQLWriter.alterSchemaStmt(tableName, avroSchema) + ";\n";
    executeQuery(alterSchemaStmt);
    log.info("Alter schema complete.");
  }

  /**
   * Add a partition to an Avro external table
   * @param tableName the name of the table to create in Hive.
   * @param partition the partition to add to the table.
   */
  public void addPartition(String tableName, int partition) throws IOException {
    String addPartitionStmt = hiveQLWriter.addPartitionStmt(tableName, partition) + ";\n";
    executeQuery(addPartitionStmt);
    log.info("Add partition complete.");
  }

  /**
   * Set the data directory of the partition
   * @param tableName the name of the table to create in Hive.
   * @param partition the partition to add to the table.
   */
  public void setPartitionLoc(String tableName, int partition) throws IOException {
    String setPartitionLocStmt = hiveQLWriter.setPartitionLocStmt(tableName, partition) + ";\n";
    executeQuery(setPartitionLocStmt);
    log.info("Set partition location complete.");
  }

  /**
   * Execute a Hive query.
   * @param query The query to execute in Hive
   * @throws IOException
   */
  public void executeQuery(String query) throws IOException {
    try {
      log.debug("Using in-process Hive instance.");
      String[] argv = getHiveArgs("-S", "-e", query);

      int status = cliDriver.run(argv);
      if (status != 0) {
        throw new IOException("Hive CliDriver exited with status=" + status);
      }
    } catch (Exception e) {
      throw new IOException("Exception thrown in Hive", e);
    }
  }

  private String[] getHiveArgs(String... args) throws IOException {
    List<String> newArgs = new LinkedList<>();
    newArgs.addAll(Arrays.asList(args));
    if (hiveConf.getBoolean(HIVE_SASL_ENABLED, false)) {
      newArgs.add("--hiveconf");
      newArgs.add("hive.metastore.sasl.enabled=true");
    }
    return newArgs.toArray(new String[newArgs.size()]);
  }
}
