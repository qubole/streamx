/**
 * Copyright 2015 Confluent Inc.
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

package io.confluent.copycat.hdfs.hive;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.TopicPartition;

import io.confluent.copycat.hdfs.FileUtils;

/**
 * Creates HiveQL statements to create external tables, add a partition, set a location to
 * a partition and modify the schema of a table.
 */
public class HiveQLWriter {

  public static final Log log = LogFactory.getLog(HiveQLWriter.class);

  private String url;
  private String topicsDir;

  /**
   * Create a writer to generate HiveQL queries
   * @param url HDFS url
   * @param topicsDir the base Avro data directory
   */

  public HiveQLWriter(String url, String topicsDir) {
    this.url = url;
    this.topicsDir = topicsDir;
  }

  /**
   * Generate a create Avro table statement.
   * @param tableName the name of the Avro table
   * @param avroSchema the schema of the Avro table
   * @return the statement to create an Avro external table.
   */
  public String createAvroTableStmt(String tableName, Schema avroSchema) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS ");

    // TODO: Add database
    sb.append(tableName);
    sb.append(" PARTITIONED BY (part int)");
    sb.append(" ROW FORMAT SERDE");
    sb.append(" 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'");

    sb.append(" STORED AS INPUTFORMAT "
              + "'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'");
    sb.append(" OUTPUTFORMAT "
              + "'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'");
    sb.append(" LOCATION ");
    sb.append("'");
    sb.append(hiveTablePath(tableName));
    sb.append("'");
    sb.append(" TBLPROPERTIES (");
    sb.append(" 'avro.schema.literal'=");
    sb.append(" '");
    sb.append(avroSchema.toString());
    sb.append(" '");
    sb.append(")");

    log.info("Create statement: " + sb.toString());
    return sb.toString();
  }

  /**
   * Generate an add partition statement.
   * @param tableName the name of the Avro table
   * @param partition the partition to add
   * @return the statement to add a partition
   */
  public String addPartitionStmt(String tableName, int partition) {
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER TABLE ");
    sb.append(tableName);
    sb.append(" ADD PARTITION (part=");
    sb.append(partition);
    sb.append(")");
    log.info("Add partition statement: " + sb.toString());
    return sb.toString();
  }

  /**
   * Generate a set partition location statement.
   * @param tableName the name of the Avro table
   * @param partition the partition to set location
   * @return the statement to set partition location
   */
  public String setPartitionLocStmt(String tableName, int partition) {
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER TABLE ");
    sb.append(tableName);
    sb.append(" PARTITION (part=");
    sb.append(partition);
    sb.append(")");
    sb.append(" SET LOCATION ");
    sb.append("'");
    sb.append(partitionPath(tableName, partition));
    sb.append("'");
    log.info("Set partition location statement: " + sb.toString());
    return sb.toString();
  }

  /**
   * Generate a alter schema statement.
   * @param tableName the name of the Avro table
   * @param avroSchema the new schema of the Avro table
   * @return the statement to alter the schema of the Avro table
   */
  public String alterSchemaStmt(String tableName, Schema avroSchema) {
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER TABLE ");
    sb.append(tableName);
    sb.append(" SET TBLPROPERTIES (");
    sb.append(" 'avro.schema.literal'=");
    sb.append(" '");
    sb.append(avroSchema.toString());
    sb.append(" '");
    sb.append(")");
    log.info("Alter schema statement: " + sb.toString());
    return sb.toString();
  }

  private String hiveTablePath(String tableName) {
    return FileUtils.hiveDirectoryName(url, topicsDir, tableName);
  }

  private String partitionPath(String tableName, int partition) {
    TopicPartition topicPart = new TopicPartition(tableName, partition);
    return FileUtils.directoryName(url, topicsDir, topicPart);
  }
}

