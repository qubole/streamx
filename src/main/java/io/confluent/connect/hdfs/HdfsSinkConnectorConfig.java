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

package io.confluent.connect.hdfs;

import java.util.Map;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;

public class HdfsSinkConnectorConfig extends AbstractConfig {

  public static final String HDFS_URL_CONFIG = "hdfs.url";
  private static final String HDFS_URL_DOC =
      "The HDFS connection URL. This configuration has the format of hdfs:://hostname:port and "
      + "specifies the HDFS to export data to.";

  public static final String HADOOP_CONF_DIR_CONFIG = "hadoop.conf.dir";
  private static final String HADOOP_CONF_DIR_DOC =
      "The Hadoop configuration directory.";
  public static final String HADOOP_CONF_DIR_DEFAULT = "";

  public static final String HADOOP_HOME_CONFIG = "hadoop.home";
  private static final String HADOOP_HOME_DOC =
      "The Hadoop home directory.";
  public static final String HADOOP_HOME_DEFAULT = "";

  public static final String FORMAT_CLASS_CONFIG = "format.class";
  private static final String FORMAT_CLASS_DOC =
      "The format class to use when writing data to HDFS. ";
  public static final String FORMAT_CLASS_DEFAULT = "io.confluent.connect.hdfs.avro.AvroFormat";

  public static final String FLUSH_SIZE_CONFIG = "flush.size";
  private static final String FLUSH_SIZE_DOC =
      "Number of records written to HDFS before invoking file commits.";

  public static final String ROTATE_INTERVAL_MS_CONFIG = "rotate.interval.ms";
  private static final String ROTATE_INTERVAL_MS_DOC =
      "The time interval in milliseconds to invoke file commits. This configuration ensures that "
      + "file commits are invoked every configured interval. This configuration is useful when data "
      + "ingestion rate is low and the connector didn't write enough messages to commit files."
      + "The default value -1 means that this feature is disabled.";
  private static final long ROTATE_INTERVAL_MS_DEFAULT = -1L;

  public static final String TOPICS_DIR_CONFIG = "topics.dir";
  private static final String TOPICS_DIR_DOC =
      "Top level HDFS directory to store the data ingested from Kafka.";
  public static final String TOPICS_DIR_DEFAULT = "topics";

  public static final String LOGS_DIR_CONFIG = "logs.dir";
  private static final String LOGS_DIR_DOC =
      "Top level HDFS directory to store the write ahead logs.";
  public static final String LOGS_DIR_DEFAULT = "logs";

  public static final String SCHEMA_CACHE_SIZE_CONFIG = "schema.cache.size";
  private static final String SCHEMA_CACHE_SIZE_DOC =
      "The size of the schema cache used in the Avro converter.";
  public static final int SCHEMA_CACHE_SIZE_DEFAULT = 1000;

  public static final String STORAGE_CLASS_CONFIG = "storage.class";
  private static final String STORAGE_CLASS_DOC =
      "The underlying storage layer. The default is HDFS";
  public static final String STORAGE_CLASS_DEFAULT = "io.confluent.connect.hdfs.storage.HdfsStorage";

  public static final String RETRY_BACKOFF_CONFIG = "retry.backoff.ms";
  private static final String RETRY_BACKOFF_DOC =
      "The retry backoff in milliseconds. This config is used to "
      + "notify Kafka connect to retry delivering a message batch or performing recovery in case "
      + "of transient exceptions.";
  public static final long RETRY_BACKOFF_DEFAULT = 5000L;

  public static final String HIVE_CONF_DIR_CONFIG = "hive.conf.dir";
  private static final String HIVE_CONF_DIR_DOC = "Hive configuration directory";
  public static final String HIVE_CONF_DIR_DEFAULT = "";

  public static final String HIVE_HOME_CONFIG = "hive.home";
  private static final String HIVE_HOME_DOC = "Hive home directory";
  public static final String HIVE_HOME_DEFAULT = "";

  public static final String HIVE_INTEGRATION_CONFIG = "hive.integration";
  private static final String HIVE_INTEGRAtiON_DOC =
      "Configuration indicating whether to integrate with Hive when running the connector.";
  public static final boolean HIVE_INTEGRATION_DEFAULT = false;

  public static final String HIVE_DATABASE_CONFIG = "hive.database";
  private static final String HIVE_DATABASE_DOC =
      "The database to use when the connector creates tables in Hive.";
  private static final String HIVE_DATABASE_DEFAULT = "default";

  public static final String HIVE_METASTORE_URIS_CONFIG = "hive.metastore.uris";
  private static final String HIVE_METASTORE_URIS_DOC =
      "The Hive metastore URIs, can be IP address or fully-qualified domain name "
      + "and port of the metastore host.";
  public static final String HIVE_METASTORE_URIS_DEFAULT = "";

  public static final String SHUTDOWN_TIMEOUT_CONFIG = "shutdown.timeout.ms";
  private static final String SHUTDOWN_TIMEOUT_DOC =
      "Clean shutdown timeout. This makes sure that asynchronous Hive metastore updates are "
      + "completed during connector shutdown.";
  private static final long SHUTDOWN_TIMEOUT_DEFAULT = 3000L;

  public static final String SCHEMA_COMPATIBILITY_CONFIG = "schema.compatibility";
  private static final String SCHEMA_COMPATIBILITY_DOC =
      "The schema compatibility rule to use when the connector is observing schema changes. The "
      + "supported configurations are NONE, BACKWARD, FORWARD and FULL.";
  private static final String SCHEMA_COMPATIBILITY_DEFAULT = "NONE";

  public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
  private static final String PARTITIONER_CLASS_DOC =
      "The partitioner to use when writing data to HDFS. You can use ``DefaultPartitioner``, "
      + "which preserves the Kafka partitions; ``FieldPartitioner``, which partitions the data to "
      + "different directories according to the value of the partitioning field specified "
      + "in ``partition.field.name``; ``TimebasedPartitioner``, which partitions data "
      + "according to the time ingested to HDFS.";
  public static final String PARTITIONER_CLASS_DEFAULT =
      "io.confluent.connect.hdfs.partitioner.DefaultPartitioner";

  public static final String PARTITION_FIELD_NAME_CONFIG = "partition.field.name";
  private static final String PARTITION_FIELD_NAME_DOC =
      "The name of the partitioning field when FieldPartitioner is used.";
  public static final String PARTITION_FIELD_NAME_DEFAULT = "";

  public static final String PARTITION_DURATION_MS_CONFIG = "partition.duration.ms";
  private static final String PARTITION_DURATION_MS_DOC =
      "The duration of a partition milliseconds used by ``TimeBasedPartitioner``. "
      + "The default value -1 means that we are not using ``TimebasedPartitioner``.";
  public static final long PARTITION_DURATION_MS_DEFAULT = -1L;

  public static final String PATH_FORMAT_CONFIG = "path.format";
  private static final String PATH_FORMAT_DOC =
      "This configuration is used to set the format of the data directories when partitioning with "
      + "``TimeBasedPartitioner``. The format set in this configuration converts the Unix timestamp "
      + "to proper directories strings. For example, if you set "
      + "``path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/``, the data directories will have"
      + " the format ``/year=2015/month=12/day=07/hour=15`` ";

  public static final String PATH_FORMAT_DEFAULT = "";

  public static final String LOCALE_CONFIG = "locale";
  private static final String LOCALE_DOC =
      "The locale to use when partitioning with ``TimeBasedPartitioner``.";
  public static final String LOCALE_DEFAULT = "";

  public static final String TIMEZONE_CONFIG = "timezone";
  private static final String TIMEZONE_DOC =
      "The timezone to use when partitioning with ``TimeBasedPartitioner``.";
  public static final String TIMEZONE_DEFAULT = "";

  public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG = "filename.offset.zero.pad.width";
  private static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC =
      "Width to zero pad offsets in HDFS filenames to if the offsets is too short in order to "
      + "provide fixed width filenames that can be ordered by simple lexicographic sorting.";
  public static final int FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT = 10;

  public static final String HDFS_AUTHENTICATION_KERBEROS_CONFIG = "hdfs.authentication.kerberos";
  private static final String HDFS_AUTHENTICATION_KERBEROS_DOC =
      "Configuration indicating whether HDFS is using Kerberos for authentication.";
  private static boolean HDFS_AUTHENTICATION_KERBEROS_DEFAULT = false;

  public static final String CONNECT_HDFS_PRINCIPAL_CONFIG = "connect.hdfs.principal";
  private static final String CONNECT_HDFS_PRINCIPAL_DOC =
      "The principal to use when HDFS is using Kerberos to for authentication.";
  public static final String CONNECT_HDFS_PRINCIPAL_DEFAULT = "";

  public static final String CONNECT_HDFS_KEYTAB_CONFIG = "connect.hdfs.keytab";
  private static final String CONNECT_HDFS_KEYTAB_DOC =
      "The path to the keytab file for the HDFS connector principal. "
      + "This keytab file should only be readable by the connector user.";
  public static final String CONNECT_HDFS_KEYTAB_DEFAULT = "";

  public static final String HDFS_NAMENODE_PRINCIPAL_CONFIG = "hdfs.namenode.principal";
  private static final String HDFS_NAMENODE_PRINCIPAL_DOC = "The principal for HDFS Namenode.";
  public static final String HDFS_NAMENODE_PRINCIPAL_DEFAULT = "";

  public static final String KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG = "kerberos.ticket.renew.period.ms";
  private static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DOC =
      "The period in milliseconds to renew the Kerberos ticket.";
  public static final long KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT = 60000 * 60;

  static ConfigDef config = new ConfigDef()
      .define(HDFS_URL_CONFIG, Type.STRING, Importance.HIGH, HDFS_URL_DOC)
      .define(HADOOP_CONF_DIR_CONFIG, Type.STRING, HADOOP_CONF_DIR_DEFAULT, Importance.HIGH,
              HADOOP_CONF_DIR_DOC)
      .define(HADOOP_HOME_CONFIG, Type.STRING, HADOOP_HOME_DEFAULT, Importance.HIGH,
              HADOOP_HOME_DOC)
      .define(FORMAT_CLASS_CONFIG, Type.STRING, FORMAT_CLASS_DEFAULT, Importance.HIGH,
              FORMAT_CLASS_DOC)
      .define(FLUSH_SIZE_CONFIG, Type.INT, Importance.HIGH, FLUSH_SIZE_DOC)
      .define(ROTATE_INTERVAL_MS_CONFIG, Type.LONG, ROTATE_INTERVAL_MS_DEFAULT,Importance.HIGH, ROTATE_INTERVAL_MS_DOC)
      .define(TOPICS_DIR_CONFIG, Type.STRING, TOPICS_DIR_DEFAULT, Importance.HIGH, TOPICS_DIR_DOC)
      .define(LOGS_DIR_CONFIG, Type.STRING, LOGS_DIR_DEFAULT, Importance.HIGH, LOGS_DIR_DOC)
      .define(SCHEMA_CACHE_SIZE_CONFIG, Type.INT, SCHEMA_CACHE_SIZE_DEFAULT, Importance.LOW,
              SCHEMA_CACHE_SIZE_DOC)
      .define(STORAGE_CLASS_CONFIG, Type.STRING, STORAGE_CLASS_DEFAULT, Importance.LOW, STORAGE_CLASS_DOC)
      .define(RETRY_BACKOFF_CONFIG, Type.LONG, RETRY_BACKOFF_DEFAULT, Importance.LOW, RETRY_BACKOFF_DOC)
      .define(HIVE_INTEGRATION_CONFIG, Type.BOOLEAN, HIVE_INTEGRATION_DEFAULT, Importance.HIGH, HIVE_INTEGRAtiON_DOC)
      .define(HIVE_HOME_CONFIG, Type.STRING, HIVE_HOME_DEFAULT, Importance.HIGH, HIVE_HOME_DOC)
      .define(HIVE_CONF_DIR_CONFIG, Type.STRING, HIVE_CONF_DIR_DEFAULT, Importance.HIGH,
              HIVE_CONF_DIR_DOC)
      .define(HIVE_DATABASE_CONFIG, Type.STRING, HIVE_DATABASE_DEFAULT, Importance.HIGH, HIVE_DATABASE_DOC)
      .define(HIVE_METASTORE_URIS_CONFIG, Type.STRING, HIVE_METASTORE_URIS_DEFAULT, Importance.HIGH, HIVE_METASTORE_URIS_DOC)
      .define(SHUTDOWN_TIMEOUT_CONFIG, Type.LONG, SHUTDOWN_TIMEOUT_DEFAULT,
              Importance.MEDIUM, SHUTDOWN_TIMEOUT_DOC)
      .define(SCHEMA_COMPATIBILITY_CONFIG, Type.STRING, SCHEMA_COMPATIBILITY_DEFAULT,
              Importance.HIGH, SCHEMA_COMPATIBILITY_DOC)
      .define(PARTITIONER_CLASS_CONFIG, Type.STRING, PARTITIONER_CLASS_DEFAULT, Importance.HIGH, PARTITIONER_CLASS_DOC)
      .define(PARTITION_FIELD_NAME_CONFIG, Type.STRING, PARTITION_FIELD_NAME_DEFAULT, Importance.MEDIUM,
              PARTITION_FIELD_NAME_DOC)
      .define(PARTITION_DURATION_MS_CONFIG, Type.LONG, PARTITION_DURATION_MS_DEFAULT, Importance.MEDIUM,
              PARTITION_DURATION_MS_DOC)
      .define(PATH_FORMAT_CONFIG, Type.STRING, PATH_FORMAT_DEFAULT, Importance.MEDIUM,
              PATH_FORMAT_DOC)
      .define(LOCALE_CONFIG, Type.STRING, LOCALE_DEFAULT, Importance.MEDIUM, LOCALE_DOC)
      .define(TIMEZONE_CONFIG, Type.STRING, TIMEZONE_DEFAULT, Importance.MEDIUM, TIMEZONE_DOC)
      .define(FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, Type.INT,
              FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT, ConfigDef.Range.atLeast(0),
              Importance.LOW, FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC)
      .define(HDFS_AUTHENTICATION_KERBEROS_CONFIG, Type.BOOLEAN,
              HDFS_AUTHENTICATION_KERBEROS_DEFAULT, Importance.HIGH,
              HDFS_AUTHENTICATION_KERBEROS_DOC)
      .define(CONNECT_HDFS_PRINCIPAL_CONFIG, Type.STRING, CONNECT_HDFS_PRINCIPAL_DEFAULT,
              Importance.HIGH, CONNECT_HDFS_PRINCIPAL_DOC)
      .define(CONNECT_HDFS_KEYTAB_CONFIG, Type.STRING, CONNECT_HDFS_KEYTAB_DEFAULT,
              Importance.HIGH, CONNECT_HDFS_KEYTAB_DOC)
      .define(HDFS_NAMENODE_PRINCIPAL_CONFIG, Type.STRING, HDFS_NAMENODE_PRINCIPAL_DEFAULT,
              Importance.HIGH, HDFS_NAMENODE_PRINCIPAL_DOC)
      .define(KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG, Type.LONG,
              KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT, Importance.LOW,
              KERBEROS_TICKET_RENEW_PERIOD_MS_DOC);

  public HdfsSinkConnectorConfig(Map<String, String> props) {
    super(config, props);
  }

  public static void main(String[] args) {
    System.out.println(config.toRst());
  }
}
