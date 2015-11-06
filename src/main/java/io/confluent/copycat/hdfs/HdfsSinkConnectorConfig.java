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

package io.confluent.copycat.hdfs;

import java.util.Properties;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Type;
import io.confluent.common.config.ConfigDef.Importance;

public class HdfsSinkConnectorConfig extends AbstractConfig {

  public static final String HDFS_URL_CONFIG = "hdfs.url";
  private static final String HDFS_URL_DOC = "HDFS connection URL.";
  public static final String RECORD_WRITER_PROVIDER_CLASS_CONFIG = "record.writer.provider.class";
  private static final String RECORD_WRITER_PROVIDER_CLASS_DOC = "";
  public static final String FLUSH_SIZE_CONFIG = "flush.size";
  private static final String FLUSH_SIZE_DOC = "The number of records needed to rotate files.";
  public static final String ROTATE_INTERVAL_CONFIG = "rotate.interval.ms";
  private static final String ROTATE_INTERVAL_DOC = "The interval to rotate files.";
  public static final String TOPIC_DIR_CONFIG = "topic.dir";
  private static final String TOPIC_DIR_DOC = "HDFS directory to store data.";
  public static final String TOPIC_DIR_DEFAULT = "topics";
  public static final String SCHEMA_CACHE_SIZE_CONFIG = "schema.cache.size";
  public static final int SCHEMA_CACHE_SIZE_DEFAULT = 1000;
  private static final String SCHEMA_CACHE_SIZE_DOC = "The size of the schema cache.";
  public static final String STORAGE_CLASS_CONFIG = "storage.class";
  private static final String STORAGE_CLASS_DOC = "The storage layer.";
  public static final String STORAGE_CLASS_DEFAULT = "io.confluent.copycat.hdfs.HdfsStorage";
  public static final String RETRY_BACKOFF_CONFIG = "retry.backoff.ms";
  private static final String RETRY_BACKOFF_DOC = "The retry backoff in milliseconds.";
  public static final long RETRY_BACKOFF_DEFALUT = 5000L;
  public static final String HIVE_HOME_CONFIG = "hive.home";
  private static final String HIVE_HOME_DOC = "Hive home directory";
  public static final String HIVE_CONF_DIR_CONFIG = "hive.conf.dir";
  private static final String HIVE_CONF_DIR_DOC = "Hive configuration directory";
  public static final String SCHEMA_COMPATIBILITY_CONFIG = "schema.compatibility";
  private static final String SCHEMA_COMPATIBILITY_DOC = "The schema compatibility level";
  private static final String SCHEMA_COMPATIBILITY_DEFAULT = "NONE";

  static ConfigDef config = new ConfigDef()
      .define(HDFS_URL_CONFIG, Type.STRING, Importance.HIGH, HDFS_URL_DOC)
      .define(RECORD_WRITER_PROVIDER_CLASS_CONFIG, Type.STRING, Importance.HIGH,
              RECORD_WRITER_PROVIDER_CLASS_DOC)
      .define(FLUSH_SIZE_CONFIG, Type.INT, Importance.HIGH, FLUSH_SIZE_DOC)
      .define(ROTATE_INTERVAL_CONFIG, Type.INT, Importance.HIGH, ROTATE_INTERVAL_DOC)
      .define(TOPIC_DIR_CONFIG, Type.STRING, TOPIC_DIR_DEFAULT, Importance.HIGH, TOPIC_DIR_DOC)
      .define(SCHEMA_CACHE_SIZE_CONFIG, Type.INT, SCHEMA_CACHE_SIZE_DEFAULT, Importance.MEDIUM, SCHEMA_CACHE_SIZE_DOC)
      .define(STORAGE_CLASS_CONFIG, Type.STRING, STORAGE_CLASS_DEFAULT, Importance.MEDIUM, STORAGE_CLASS_DOC)
      .define(RETRY_BACKOFF_CONFIG, Type.LONG, RETRY_BACKOFF_DEFALUT, Importance.HIGH, RETRY_BACKOFF_DOC)
      .define(HIVE_HOME_CONFIG, Type.STRING, Importance.HIGH, HIVE_HOME_DOC)
      .define(HIVE_CONF_DIR_CONFIG, Type.STRING, Importance.HIGH, HIVE_CONF_DIR_DOC)
      .define(SCHEMA_COMPATIBILITY_CONFIG, Type.STRING, SCHEMA_COMPATIBILITY_DEFAULT, Importance.HIGH, SCHEMA_COMPATIBILITY_DOC);

  HdfsSinkConnectorConfig(Properties props) {
    super(config, props);
  }
}
