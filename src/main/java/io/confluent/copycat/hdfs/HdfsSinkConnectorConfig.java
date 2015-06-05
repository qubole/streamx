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
  public static final String TOPIC_DIR_CONFIG = "topic.dir";
  private static final String TOPIC_DIR_DOC = "HDFS directory to store data";
  public static final String TOPIC_DIR_DEFAULT = "topics";

  static ConfigDef config = new ConfigDef()
      .define(HDFS_URL_CONFIG, Type.STRING, Importance.HIGH, HDFS_URL_DOC)
      .define(RECORD_WRITER_PROVIDER_CLASS_CONFIG, Type.STRING, Importance.HIGH, RECORD_WRITER_PROVIDER_CLASS_DOC)
      .define(FLUSH_SIZE_CONFIG, Type.INT, Importance.HIGH, FLUSH_SIZE_DOC)
      .define(TOPIC_DIR_CONFIG, Type.STRING, TOPIC_DIR_DEFAULT, Importance.HIGH, TOPIC_DIR_DOC);

  HdfsSinkConnectorConfig(Properties props) {
    super(config, props);
  }
}
