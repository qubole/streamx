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

package io.confluent.connect.hdfs.hive;

import org.apache.kafka.connect.data.Schema;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.partitioner.Partitioner;

public abstract class HiveUtil {

  protected final String url;
  protected final String topicsDir;
  protected final AvroData avroData;
  protected final HiveMetaStore hiveMetaStore;


  public HiveUtil(HdfsSinkConnectorConfig connectorConfig, AvroData avroData, HiveMetaStore hiveMetaStore) {
    this.url = connectorConfig.getString(HdfsSinkConnectorConfig.HDFS_URL_CONFIG);
    this.topicsDir = connectorConfig.getString(HdfsSinkConnectorConfig.TOPICS_DIR_CONFIG);
    this.avroData = avroData;
    this.hiveMetaStore = hiveMetaStore;
  }

  public abstract void createTable(String database, String tableName, Schema schema, Partitioner partitioner);

  public abstract void alterSchema(String database, String tableName, Schema schema);
}
