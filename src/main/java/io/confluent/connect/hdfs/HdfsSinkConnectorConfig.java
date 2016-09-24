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

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.confluent.connect.hdfs.partitioner.DailyPartitioner;
import io.confluent.connect.hdfs.partitioner.FieldPartitioner;
import io.confluent.connect.hdfs.partitioner.HourlyPartitioner;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.hdfs.partitioner.TimeBasedPartitioner;

public class HdfsSinkConnectorConfig extends AbstractConfig {

  // HDFS Group
  public static final String HDFS_URL_CONFIG = "hdfs.url";
  private static final String HDFS_URL_DOC =
      "The HDFS connection URL. This configuration has the format of hdfs:://hostname:port and "
      + "specifies the HDFS to export data to.";
  private static final String HDFS_URL_DISPLAY = "HDFS URL";

  public static final String HADOOP_CONF_DIR_CONFIG = "hadoop.conf.dir";
  private static final String HADOOP_CONF_DIR_DOC =
      "The Hadoop configuration directory.";
  public static final String HADOOP_CONF_DIR_DEFAULT = "";
  private static final String HADOOP_CONF_DIR_DISPLAY = "Hadoop Configuration Directory";

  public static final String HADOOP_HOME_CONFIG = "hadoop.home";
  private static final String HADOOP_HOME_DOC =
      "The Hadoop home directory.";
  public static final String HADOOP_HOME_DEFAULT = "";
  private static final String HADOOP_HOME_DISPLAY = "Hadoop home directory";

  public static final String TOPICS_DIR_CONFIG = "topics.dir";
  private static final String TOPICS_DIR_DOC =
      "Top level HDFS directory to store the data ingested from Kafka.";
  public static final String TOPICS_DIR_DEFAULT = "topics";
  private static final String TOPICS_DIR_DISPLAY = "Topics directory";

  public static final String LOGS_DIR_CONFIG = "logs.dir";
  private static final String LOGS_DIR_DOC =
      "Top level HDFS directory to store the write ahead logs.";
  public static final String LOGS_DIR_DEFAULT = "logs";
  private static final String LOGS_DIR_DISPLAY = "Logs directory";

  public static final String FORMAT_CLASS_CONFIG = "format.class";
  private static final String FORMAT_CLASS_DOC =
      "The format class to use when writing data to HDFS. ";
  public static final String FORMAT_CLASS_DEFAULT = "io.confluent.connect.hdfs.avro.AvroFormat";
  private static final String FORMAT_CLASS_DISPLAY = "Format class";

  // Hive group
  public static final String HIVE_INTEGRATION_CONFIG = "hive.integration";
  private static final String HIVE_INTEGRATION_DOC =
      "Configuration indicating whether to integrate with Hive when running the connector.";
  public static final boolean HIVE_INTEGRATION_DEFAULT = false;
  private static final String HIVE_INTEGRATION_DISPLAY = "Hive Integration";

  public static final String HIVE_METASTORE_URIS_CONFIG = "hive.metastore.uris";
  private static final String HIVE_METASTORE_URIS_DOC =
      "The Hive metastore URIs, can be IP address or fully-qualified domain name "
      + "and port of the metastore host.";
  public static final String HIVE_METASTORE_URIS_DEFAULT = "";
  private static final String HIVE_METASTORE_URIS_DISPLAY = "Hive Metastore URIs";

  public static final String HIVE_CONF_DIR_CONFIG = "hive.conf.dir";
  private static final String HIVE_CONF_DIR_DOC = "Hive configuration directory";
  public static final String HIVE_CONF_DIR_DEFAULT = "";
  private static final String HIVE_CONF_DIR_DISPLAY = "Hive configuration directory";

  public static final String HIVE_HOME_CONFIG = "hive.home";
  private static final String HIVE_HOME_DOC = "Hive home directory";
  public static final String HIVE_HOME_DEFAULT = "";
  private static final String HIVE_HOME_DISPLAY = "Hive home directory";

  public static final String HIVE_DATABASE_CONFIG = "hive.database";
  private static final String HIVE_DATABASE_DOC =
      "The database to use when the connector creates tables in Hive.";
  private static final String HIVE_DATABASE_DEFAULT = "default";
  private static final String HIVE_DATABASE_DISPLAY = "Hive database";

  // Security group
  public static final String HDFS_AUTHENTICATION_KERBEROS_CONFIG = "hdfs.authentication.kerberos";
  private static final String HDFS_AUTHENTICATION_KERBEROS_DOC =
      "Configuration indicating whether HDFS is using Kerberos for authentication.";
  private static final boolean HDFS_AUTHENTICATION_KERBEROS_DEFAULT = false;
  private static final String HDFS_AUTHENTICATION_KERBEROS_DISPLAY = "HDFS Authentication Kerberos";

  public static final String CONNECT_HDFS_PRINCIPAL_CONFIG = "connect.hdfs.principal";
  private static final String CONNECT_HDFS_PRINCIPAL_DOC =
      "The principal to use when HDFS is using Kerberos to for authentication.";
  public static final String CONNECT_HDFS_PRINCIPAL_DEFAULT = "";
  private static final String CONNECT_HDFS_PRINCIPAL_DISPLAY = "Connect Kerberos Principal";

  public static final String CONNECT_HDFS_KEYTAB_CONFIG = "connect.hdfs.keytab";
  private static final String CONNECT_HDFS_KEYTAB_DOC =
      "The path to the keytab file for the HDFS connector principal. "
      + "This keytab file should only be readable by the connector user.";
  public static final String CONNECT_HDFS_KEYTAB_DEFAULT = "";
  private static final String CONNECT_HDFS_KEYTAB_DISPLAY = "Connect Kerberos Keytab";

  public static final String HDFS_NAMENODE_PRINCIPAL_CONFIG = "hdfs.namenode.principal";
  private static final String HDFS_NAMENODE_PRINCIPAL_DOC = "The principal for HDFS Namenode.";
  public static final String HDFS_NAMENODE_PRINCIPAL_DEFAULT = "";
  private static final String HDFS_NAMENODE_PRINCIPAL_DISPLAY = "HDFS NameNode Kerberos Principal";

  public static final String KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG = "kerberos.ticket.renew.period.ms";
  private static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DOC =
      "The period in milliseconds to renew the Kerberos ticket.";
  public static final long KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT = 60000 * 60;
  private static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DISPLAY = "Kerberos Ticket Renew Period (ms)";

  // Connector group
  public static final String FLUSH_SIZE_CONFIG = "flush.size";
  private static final String FLUSH_SIZE_DOC =
      "Number of records written to HDFS before invoking file commits.";
  private static final String FLUSH_SIZE_DISPLAY = "Flush Size";

  public static final String ROTATE_INTERVAL_MS_CONFIG = "rotate.interval.ms";
  private static final String ROTATE_INTERVAL_MS_DOC =
      "The time interval in milliseconds to invoke file commits. This configuration ensures that "
      + "file commits are invoked every configured interval. This configuration is useful when data "
      + "ingestion rate is low and the connector didn't write enough messages to commit files."
      + "The default value -1 means that this feature is disabled.";
  private static final long ROTATE_INTERVAL_MS_DEFAULT = -1L;
  private static final String ROTATE_INTERVAL_MS_DISPLAY = "Rotate Interval (ms)";

  public static final String ROTATE_SCHEDULE_INTERVAL_MS_CONFIG = "rotate.schedule.interval.ms";
  private static final String ROTATE_SCHEDULE_INTERVAL_MS_DOC =
          "The time interval in milliseconds to periodically invoke file commits. This configuration ensures that "
          + "file commits are invoked every configured interval. Time of commit will be adjusted to 00:00 of selected timezone. "
          + "Commit will be performed at scheduled time regardless previous commit time or number of messages. "
          + "This configuration is useful when you have to commit your data based on current server time, like at the beginning of every hour. "
          + "The default value -1 means that this feature is disabled.";
  private static final long ROTATE_SCHEDULE_INTERVAL_MS_DEFAULT = -1L;
  private static final String ROTATE_SCHEDULE_INTERVAL_MS_DISPLAY = "Rotate Schedule Interval (ms)";

  public static final String RETRY_BACKOFF_CONFIG = "retry.backoff.ms";
  private static final String RETRY_BACKOFF_DOC =
      "The retry backoff in milliseconds. This config is used to "
      + "notify Kafka connect to retry delivering a message batch or performing recovery in case "
      + "of transient exceptions.";
  public static final long RETRY_BACKOFF_DEFAULT = 5000L;
  private static final String RETRY_BACKOFF_DISPLAY = "Retry Backoff (ms)";

  public static final String SHUTDOWN_TIMEOUT_CONFIG = "shutdown.timeout.ms";
  private static final String SHUTDOWN_TIMEOUT_DOC =
      "Clean shutdown timeout. This makes sure that asynchronous Hive metastore updates are "
      + "completed during connector shutdown.";
  private static final long SHUTDOWN_TIMEOUT_DEFAULT = 3000L;
  private static final String SHUTDOWN_TIMEOUT_DISPLAY = "Shutdown Timeout (ms)";

  public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
  private static final String PARTITIONER_CLASS_DOC =
      "The partitioner to use when writing data to HDFS. You can use ``DefaultPartitioner``, "
      + "which preserves the Kafka partitions; ``FieldPartitioner``, which partitions the data to "
      + "different directories according to the value of the partitioning field specified "
      + "in ``partition.field.name``; ``TimebasedPartitioner``, which partitions data "
      + "according to the time ingested to HDFS.";
  public static final String PARTITIONER_CLASS_DEFAULT =
      "io.confluent.connect.hdfs.partitioner.DefaultPartitioner";
  private static final String PARTITIONER_CLASS_DISPLAY = "Partitioner Class";

  public static final String PARTITION_FIELD_NAME_CONFIG = "partition.field.name";
  private static final String PARTITION_FIELD_NAME_DOC =
      "The name of the partitioning field when FieldPartitioner is used.";
  public static final String PARTITION_FIELD_NAME_DEFAULT = "";
  public static final String PARTITION_FIELD_NAME_DISPLAY = "Partition Field Name";

  public static final String PARTITION_DURATION_MS_CONFIG = "partition.duration.ms";
  private static final String PARTITION_DURATION_MS_DOC =
      "The duration of a partition milliseconds used by ``TimeBasedPartitioner``. "
      + "The default value -1 means that we are not using ``TimebasedPartitioner``.";
  public static final long PARTITION_DURATION_MS_DEFAULT = -1L;
  private static final String PARTITION_DURATION_MS_DISPLAY = "Partition Duration (ms)";

  public static final String PATH_FORMAT_CONFIG = "path.format";
  private static final String PATH_FORMAT_DOC =
      "This configuration is used to set the format of the data directories when partitioning with "
      + "``TimeBasedPartitioner``. The format set in this configuration converts the Unix timestamp "
      + "to proper directories strings. For example, if you set "
      + "``path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/``, the data directories will have"
      + " the format ``/year=2015/month=12/day=07/hour=15`` ";
  public static final String PATH_FORMAT_DEFAULT = "";
  private static final String PATH_FORMAT_DISPLAY = "Path Format";

  public static final String LOCALE_CONFIG = "locale";
  private static final String LOCALE_DOC =
      "The locale to use when partitioning with ``TimeBasedPartitioner``.";
  public static final String LOCALE_DEFAULT = "";
  private static final String LOCALE_DISPLAY = "Locale";

  public static final String TIMEZONE_CONFIG = "timezone";
  private static final String TIMEZONE_DOC =
      "The timezone to use when partitioning with ``TimeBasedPartitioner``.";
  public static final String TIMEZONE_DEFAULT = "";
  private static final String TIMEZONE_DISPLAY = "Timezone";

  public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG = "filename.offset.zero.pad.width";
  private static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC =
      "Width to zero pad offsets in HDFS filenames to if the offsets is too short in order to "
      + "provide fixed width filenames that can be ordered by simple lexicographic sorting.";
  public static final int FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT = 10;
  private static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY = "Filename Offset Zero Pad Width";

  // Schema group
  public static final String SCHEMA_COMPATIBILITY_CONFIG = "schema.compatibility";
  private static final String SCHEMA_COMPATIBILITY_DOC =
      "The schema compatibility rule to use when the connector is observing schema changes. The "
      + "supported configurations are NONE, BACKWARD, FORWARD and FULL.";
  private static final String SCHEMA_COMPATIBILITY_DEFAULT = "NONE";
  private static final String SCHEMA_COMPATIBILITY_DISPLAY = "Schema Compatibility";

  public static final String SCHEMA_CACHE_SIZE_CONFIG = "schema.cache.size";
  private static final String SCHEMA_CACHE_SIZE_DOC =
      "The size of the schema cache used in the Avro converter.";
  public static final int SCHEMA_CACHE_SIZE_DEFAULT = 1000;
  private static final String SCHEMA_CACHE_SIZE_DISPLAY = "Schema Cache Size";

  // Internal group
  public static final String STORAGE_CLASS_CONFIG = "storage.class";
  private static final String STORAGE_CLASS_DOC =
      "The underlying storage layer. The default is HDFS";
  public static final String STORAGE_CLASS_DEFAULT = "io.confluent.connect.hdfs.storage.HdfsStorage";
  private static final String STORAGE_CLASS_DISPLAY = "Storage Class";

  public static final String HDFS_GROUP = "HDFS";
  public static final String HIVE_GROUP = "Hive";
  public static final String SECURITY_GROUP = "Security";
  public static final String SCHEMA_GROUP = "Schema";
  public static final String CONNECTOR_GROUP = "Connector";
  public static final String INTERNAL_GROUP = "Internal";

  private static final ConfigDef.Recommender hiveIntegrationDependentsRecommender = new BooleanParentRecommender(HIVE_INTEGRATION_CONFIG);
  private static final ConfigDef.Recommender hdfsAuthenticationKerberosDependentsRecommender = new BooleanParentRecommender(HDFS_AUTHENTICATION_KERBEROS_CONFIG);
  private static final ConfigDef.Recommender partitionerClassDependentsRecommender = new PartitionerClassDependentsRecommender();
  private static final ConfigDef.Recommender schemaCompatibilityRecommender = new SchemaCompatibilityRecommender();

  private static ConfigDef config = new ConfigDef();

  static {

    // Define HDFS configuration group
    config.define(HDFS_URL_CONFIG, Type.STRING, Importance.HIGH, HDFS_URL_DOC, HDFS_GROUP, 1, Width.MEDIUM, HDFS_URL_DISPLAY)
        .define(HADOOP_CONF_DIR_CONFIG, Type.STRING, HADOOP_CONF_DIR_DEFAULT, Importance.HIGH, HADOOP_CONF_DIR_DOC, HDFS_GROUP, 2, Width.MEDIUM, HADOOP_CONF_DIR_DISPLAY)
        .define(HADOOP_HOME_CONFIG, Type.STRING, HADOOP_HOME_DEFAULT, Importance.HIGH, HADOOP_HOME_DOC, HDFS_GROUP, 3, Width.SHORT, HADOOP_HOME_DISPLAY)
        .define(TOPICS_DIR_CONFIG, Type.STRING, TOPICS_DIR_DEFAULT, Importance.HIGH, TOPICS_DIR_DOC, HDFS_GROUP, 4, Width.SHORT, TOPICS_DIR_DISPLAY)
        .define(LOGS_DIR_CONFIG, Type.STRING, LOGS_DIR_DEFAULT, Importance.HIGH, LOGS_DIR_DOC, HDFS_GROUP, 5, Width.SHORT, LOGS_DIR_DISPLAY)
        .define(FORMAT_CLASS_CONFIG, Type.STRING, FORMAT_CLASS_DEFAULT, Importance.HIGH, FORMAT_CLASS_DOC, HDFS_GROUP, 6, Width.SHORT, FORMAT_CLASS_DISPLAY);

    // Define Hive configuration group
    config.define(HIVE_INTEGRATION_CONFIG, Type.BOOLEAN, HIVE_INTEGRATION_DEFAULT, Importance.HIGH, HIVE_INTEGRATION_DOC, HIVE_GROUP, 1, Width.SHORT, HIVE_INTEGRATION_DISPLAY,
                  Arrays.asList(HIVE_METASTORE_URIS_CONFIG, HIVE_CONF_DIR_CONFIG, HIVE_HOME_CONFIG, HIVE_DATABASE_CONFIG, SCHEMA_COMPATIBILITY_CONFIG))
        .define(HIVE_METASTORE_URIS_CONFIG, Type.STRING, HIVE_METASTORE_URIS_DEFAULT, Importance.HIGH, HIVE_METASTORE_URIS_DOC, HIVE_GROUP, 2, Width.MEDIUM,
                HIVE_METASTORE_URIS_DISPLAY, hiveIntegrationDependentsRecommender)
        .define(HIVE_CONF_DIR_CONFIG, Type.STRING, HIVE_CONF_DIR_DEFAULT, Importance.HIGH, HIVE_CONF_DIR_DOC, HIVE_GROUP, 3, Width.MEDIUM, HIVE_CONF_DIR_DISPLAY, hiveIntegrationDependentsRecommender)
        .define(HIVE_HOME_CONFIG, Type.STRING, HIVE_HOME_DEFAULT, Importance.HIGH, HIVE_HOME_DOC, HIVE_GROUP, 4, Width.MEDIUM, HIVE_HOME_DISPLAY, hiveIntegrationDependentsRecommender)
        .define(HIVE_DATABASE_CONFIG, Type.STRING, HIVE_DATABASE_DEFAULT, Importance.HIGH, HIVE_DATABASE_DOC, HIVE_GROUP, 5, Width.SHORT, HIVE_DATABASE_DISPLAY, hiveIntegrationDependentsRecommender);

    // Define Security configuration group
    config.define(HDFS_AUTHENTICATION_KERBEROS_CONFIG, Type.BOOLEAN, HDFS_AUTHENTICATION_KERBEROS_DEFAULT, Importance.HIGH, HDFS_AUTHENTICATION_KERBEROS_DOC,
                  SECURITY_GROUP, 1, Width.SHORT, HDFS_AUTHENTICATION_KERBEROS_DISPLAY,
                  Arrays.asList(CONNECT_HDFS_PRINCIPAL_CONFIG, CONNECT_HDFS_KEYTAB_CONFIG, HDFS_NAMENODE_PRINCIPAL_CONFIG, KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG))
        .define(CONNECT_HDFS_PRINCIPAL_CONFIG, Type.STRING, CONNECT_HDFS_PRINCIPAL_DEFAULT, Importance.HIGH, CONNECT_HDFS_PRINCIPAL_DOC,
                SECURITY_GROUP, 2, Width.MEDIUM, CONNECT_HDFS_PRINCIPAL_DISPLAY, hdfsAuthenticationKerberosDependentsRecommender)
        .define(CONNECT_HDFS_KEYTAB_CONFIG, Type.STRING, CONNECT_HDFS_KEYTAB_DEFAULT, Importance.HIGH, CONNECT_HDFS_KEYTAB_DOC,
                SECURITY_GROUP, 3, Width.MEDIUM, CONNECT_HDFS_KEYTAB_DISPLAY, hdfsAuthenticationKerberosDependentsRecommender)
        .define(HDFS_NAMENODE_PRINCIPAL_CONFIG, Type.STRING, HDFS_NAMENODE_PRINCIPAL_DEFAULT, Importance.HIGH, HDFS_NAMENODE_PRINCIPAL_DOC,
                SECURITY_GROUP, 4, Width.MEDIUM, HDFS_NAMENODE_PRINCIPAL_DISPLAY, hdfsAuthenticationKerberosDependentsRecommender)
        .define(KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG, Type.LONG, KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT, Importance.LOW, KERBEROS_TICKET_RENEW_PERIOD_MS_DOC,
                SECURITY_GROUP, 5, Width.SHORT, KERBEROS_TICKET_RENEW_PERIOD_MS_DISPLAY, hdfsAuthenticationKerberosDependentsRecommender);

    // Define Schema configuration group
    config.define(SCHEMA_COMPATIBILITY_CONFIG, Type.STRING, SCHEMA_COMPATIBILITY_DEFAULT, Importance.HIGH, SCHEMA_COMPATIBILITY_DOC, SCHEMA_GROUP, 1, Width.SHORT,
                  SCHEMA_COMPATIBILITY_DISPLAY, schemaCompatibilityRecommender)
        .define(SCHEMA_CACHE_SIZE_CONFIG, Type.INT, SCHEMA_CACHE_SIZE_DEFAULT, Importance.LOW, SCHEMA_CACHE_SIZE_DOC, SCHEMA_GROUP, 2, Width.SHORT, SCHEMA_CACHE_SIZE_DISPLAY);

    // Define Connector configuration group
    config.define(FLUSH_SIZE_CONFIG, Type.INT, Importance.HIGH, FLUSH_SIZE_DOC, CONNECTOR_GROUP, 1, Width.SHORT, FLUSH_SIZE_DISPLAY)
        .define(ROTATE_INTERVAL_MS_CONFIG, Type.LONG, ROTATE_INTERVAL_MS_DEFAULT, Importance.HIGH, ROTATE_INTERVAL_MS_DOC, CONNECTOR_GROUP, 2, Width.SHORT, ROTATE_INTERVAL_MS_DISPLAY)
        .define(ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, Type.LONG, ROTATE_SCHEDULE_INTERVAL_MS_DEFAULT, Importance.MEDIUM, ROTATE_SCHEDULE_INTERVAL_MS_DOC, CONNECTOR_GROUP, 3, Width.SHORT, ROTATE_SCHEDULE_INTERVAL_MS_DISPLAY)
        .define(RETRY_BACKOFF_CONFIG, Type.LONG, RETRY_BACKOFF_DEFAULT, Importance.LOW, RETRY_BACKOFF_DOC, CONNECTOR_GROUP, 4, Width.SHORT, RETRY_BACKOFF_DISPLAY)
        .define(SHUTDOWN_TIMEOUT_CONFIG, Type.LONG, SHUTDOWN_TIMEOUT_DEFAULT, Importance.MEDIUM, SHUTDOWN_TIMEOUT_DOC, CONNECTOR_GROUP, 5, Width.SHORT, SHUTDOWN_TIMEOUT_DISPLAY)
        .define(PARTITIONER_CLASS_CONFIG, Type.STRING, PARTITIONER_CLASS_DEFAULT, Importance.HIGH, PARTITIONER_CLASS_DOC, CONNECTOR_GROUP, 6, Width.LONG, PARTITIONER_CLASS_DISPLAY,
                Arrays.asList(PARTITION_FIELD_NAME_CONFIG, PARTITION_DURATION_MS_CONFIG, PATH_FORMAT_CONFIG, LOCALE_CONFIG, TIMEZONE_CONFIG))
        .define(PARTITION_FIELD_NAME_CONFIG, Type.STRING, PARTITION_FIELD_NAME_DEFAULT, Importance.MEDIUM, PARTITION_FIELD_NAME_DOC, CONNECTOR_GROUP, 7, Width.MEDIUM,
                PARTITION_FIELD_NAME_DISPLAY, partitionerClassDependentsRecommender)
        .define(PARTITION_DURATION_MS_CONFIG, Type.LONG, PARTITION_DURATION_MS_DEFAULT, Importance.MEDIUM, PARTITION_DURATION_MS_DOC, CONNECTOR_GROUP, 8, Width.SHORT,
                PARTITION_DURATION_MS_DISPLAY, partitionerClassDependentsRecommender)
        .define(PATH_FORMAT_CONFIG, Type.STRING, PATH_FORMAT_DEFAULT, Importance.MEDIUM, PATH_FORMAT_DOC, CONNECTOR_GROUP, 9, Width.LONG, PATH_FORMAT_DISPLAY,
                partitionerClassDependentsRecommender)
        .define(LOCALE_CONFIG, Type.STRING, LOCALE_DEFAULT, Importance.MEDIUM, LOCALE_DOC, CONNECTOR_GROUP, 10, Width.MEDIUM, LOCALE_DISPLAY, partitionerClassDependentsRecommender)
        .define(TIMEZONE_CONFIG, Type.STRING, TIMEZONE_DEFAULT, Importance.MEDIUM, TIMEZONE_DOC, CONNECTOR_GROUP, 11, Width.MEDIUM, TIMEZONE_DISPLAY, partitionerClassDependentsRecommender)
        .define(FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, Type.INT, FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT, ConfigDef.Range.atLeast(0), Importance.LOW, FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC,
                CONNECTOR_GROUP, 12, Width.SHORT, FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY);

    // Define Internal configuration group
    config.define(STORAGE_CLASS_CONFIG, Type.STRING, STORAGE_CLASS_DEFAULT, Importance.LOW, STORAGE_CLASS_DOC, INTERNAL_GROUP, 1, Width.MEDIUM, STORAGE_CLASS_DISPLAY);
  }

  private static class SchemaCompatibilityRecommender extends BooleanParentRecommender {
    
    public SchemaCompatibilityRecommender() {
      super(HIVE_INTEGRATION_CONFIG);
    }
      
    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      boolean hiveIntegration = (Boolean) connectorConfigs.get(parentConfigName);
      if (hiveIntegration) {
        return Arrays.<Object>asList("BACKWARD", "FORWARD", "FULL");
      } else {
        return Arrays.<Object>asList("NONE", "BACKWARD", "FORWARD", "FULL");
      }
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return true;
    }
  }
  
  private static class BooleanParentRecommender implements ConfigDef.Recommender {
    
    protected String parentConfigName;
    
    public BooleanParentRecommender(String parentConfigName) {
      this.parentConfigName = parentConfigName;
    }
    
    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      return new LinkedList<>();
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return (Boolean) connectorConfigs.get(parentConfigName);
    }
  }

  private static class PartitionerClassDependentsRecommender implements ConfigDef.Recommender {

    @Override
    public List<Object> validValues(String name, Map<String, Object> props) {
      return new LinkedList<>();
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      String partitionerName = (String) connectorConfigs.get(PARTITIONER_CLASS_CONFIG);
      try {
        @SuppressWarnings("unchecked")
        Class<? extends Partitioner> partitioner = (Class<? extends Partitioner>) Class.forName(partitionerName);
        if (classNameEquals(partitionerName, DefaultPartitioner.class)) {
          return false;
        } else if (FieldPartitioner.class.isAssignableFrom(partitioner)) {
          // subclass of FieldPartitioner
          return name.equals(PARTITION_FIELD_NAME_CONFIG);
        } else if (TimeBasedPartitioner.class.isAssignableFrom(partitioner)) {
          // subclass of TimeBasedPartitioner
          if (classNameEquals(partitionerName, DailyPartitioner.class) || classNameEquals(partitionerName, HourlyPartitioner.class)) {
            return name.equals(LOCALE_CONFIG) || name.equals(TIMEZONE_CONFIG);
          } else {
            return name.equals(PARTITION_DURATION_MS_CONFIG) || name.equals(PATH_FORMAT_CONFIG) || name.equals(LOCALE_CONFIG) || name.equals(TIMEZONE_CONFIG);
          }
        } else {
          throw new ConfigException("Not a valid partitioner class: " + partitionerName);
        }
      } catch (ClassNotFoundException e) {
        throw new ConfigException("Partitioner class not found: " + partitionerName);
      }
    }
  }

  private static boolean classNameEquals(String className, Class<?> clazz) {
    return className.equals(clazz.getSimpleName()) || className.equals(clazz.getCanonicalName());
  }

  public static ConfigDef getConfig() {
    return config;
  }

  public HdfsSinkConnectorConfig(Map<String, String> props) {
    super(config, props);
  }

  public static void main(String[] args) {
    System.out.println(config.toRst());
  }
}
