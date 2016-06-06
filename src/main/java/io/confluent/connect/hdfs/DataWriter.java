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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.filter.CommittedFileFilter;
import io.confluent.connect.hdfs.filter.TopicCommittedFileFilter;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.hdfs.storage.StorageFactory;

public class DataWriter {
  private static final Logger log = LoggerFactory.getLogger(DataWriter.class);

  private Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;
  private String url;
  private Storage storage;
  private Configuration conf;
  private String topicsDir;
  private Format format;
  private Set<TopicPartition> assignment;
  private Partitioner partitioner;
  private RecordWriterProvider writerProvider;
  private SchemaFileReader schemaFileReader;
  private Map<TopicPartition, Long> offsets;
  private HdfsSinkConnectorConfig connectorConfig;
  private AvroData avroData;
  private SinkTaskContext context;
  private ExecutorService executorService;
  private String hiveDatabase;
  private HiveMetaStore hiveMetaStore;
  private HiveUtil hive;
  private Queue<Future> hiveUpdateFutures;
  private boolean hiveIntegration;
  private Thread ticketRenewThread;
  private volatile boolean isRunning;

  @SuppressWarnings("unchecked")
  public DataWriter(HdfsSinkConnectorConfig connectorConfig, SinkTaskContext context, AvroData avroData) {
    try {
      String hadoopHome = connectorConfig.getString(HdfsSinkConnectorConfig.HADOOP_HOME_CONFIG);
      System.setProperty("hadoop.home.dir", hadoopHome);

      this.connectorConfig = connectorConfig;
      this.avroData = avroData;
      this.context = context;

      String hadoopConfDir = connectorConfig.getString(HdfsSinkConnectorConfig.HADOOP_CONF_DIR_CONFIG);
      log.info("Hadoop configuration directory {}", hadoopConfDir);
      conf = new Configuration();
      if (!hadoopConfDir.equals("")) {
        conf.addResource(new Path(hadoopConfDir + "/core-site.xml"));
        conf.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"));
      }

      boolean secureHadoop = connectorConfig.getBoolean(HdfsSinkConnectorConfig.HDFS_AUTHENTICATION_KERBEROS_CONFIG);
      if (secureHadoop) {
        SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
        String principalConfig = connectorConfig.getString(HdfsSinkConnectorConfig.CONNECT_HDFS_PRINCIPAL_CONFIG);
        String keytab = connectorConfig.getString(HdfsSinkConnectorConfig.CONNECT_HDFS_KEYTAB_CONFIG);

        if (principalConfig == null || keytab == null) {
          throw new ConfigException(
              "Hadoop is using Kerboros for authentication, you need to provide both a connect principal and "
              + "the path to the keytab of the principal.");
        }

        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.security.authorization", "true");
        String hostname = InetAddress.getLocalHost().getCanonicalHostName();
        // replace the _HOST specified in the principal config to the actual host
        String principal = SecurityUtil.getServerPrincipal(principalConfig, hostname);
        String namenodePrincipalConfig = connectorConfig.getString(HdfsSinkConnectorConfig.HDFS_NAMENODE_PRINCIPAL_CONFIG);

        String namenodePrincipal = SecurityUtil.getServerPrincipal(namenodePrincipalConfig, hostname);
        // namenode principal is needed for multi-node hadoop cluster
        if (conf.get("dfs.namenode.kerberos.principal") == null) {
          conf.set("dfs.namenode.kerberos.principal", namenodePrincipal);
        }
        log.info("Hadoop namenode principal: " + conf.get("dfs.namenode.kerberos.principal"));

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
        final UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        log.info("Login as: " + ugi.getUserName());

        final long renewPeriod = connectorConfig.getLong(HdfsSinkConnectorConfig.KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG);

        isRunning = true;
        ticketRenewThread = new Thread(new Runnable() {
          @Override
          public void run() {
            synchronized (DataWriter.this) {
              while (isRunning) {
                try {
                  DataWriter.this.wait(renewPeriod);
                  if (isRunning) {
                    ugi.reloginFromKeytab();
                  }
                } catch (IOException e) {
                  // We ignore this exception during relogin as each successful relogin gives
                  // additional 24 hours of authentication in the default config. In normal
                  // situations, the probability of failing relogin 24 times is low and if
                  // that happens, the task will fail eventually.
                  log.error("Error renewing the ticket", e);
                } catch (InterruptedException e) {
                  // ignored
                }
              }
            }
          }
        });
        log.info("Starting the Kerberos ticket renew thread with period {}ms.", renewPeriod);
        ticketRenewThread.start();
      }

      url = connectorConfig.getString(HdfsSinkConnectorConfig.HDFS_URL_CONFIG);
      topicsDir = connectorConfig.getString(HdfsSinkConnectorConfig.TOPICS_DIR_CONFIG);
      String logsDir = connectorConfig.getString(HdfsSinkConnectorConfig.LOGS_DIR_CONFIG);

      Class<? extends Storage> storageClass = (Class<? extends Storage>) Class
              .forName(connectorConfig.getString(HdfsSinkConnectorConfig.STORAGE_CLASS_CONFIG));
      storage = StorageFactory.createStorage(storageClass, conf, url);

      createDir(topicsDir);
      createDir(topicsDir + HdfsSinkConnectorConstants.TEMPFILE_DIRECTORY);
      createDir(logsDir);

      format = getFormat();
      writerProvider = format.getRecordWriterProvider();
      schemaFileReader = format.getSchemaFileReader(avroData);

      partitioner = createPartitioner(connectorConfig);

      assignment = new HashSet<>(context.assignment());
      offsets = new HashMap<>();

      hiveIntegration = connectorConfig.getBoolean(HdfsSinkConnectorConfig.HIVE_INTEGRATION_CONFIG);
      if (hiveIntegration) {
        hiveDatabase = connectorConfig.getString(HdfsSinkConnectorConfig.HIVE_DATABASE_CONFIG);
        hiveMetaStore = new HiveMetaStore(conf, connectorConfig);
        hive = format.getHiveUtil(connectorConfig, avroData, hiveMetaStore);
        executorService = Executors.newSingleThreadExecutor();
        hiveUpdateFutures = new LinkedList<>();
      }

      topicPartitionWriters = new HashMap<>();
      for (TopicPartition tp: assignment) {
        TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
            tp, storage, writerProvider, partitioner, connectorConfig, context, avroData, hiveMetaStore, hive, schemaFileReader, executorService,
            hiveUpdateFutures);
        topicPartitionWriters.put(tp, topicPartitionWriter);
      }
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new ConnectException("Reflection exception: ", e);
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  public void write(Collection<SinkRecord> records) {
    for (SinkRecord record: records) {
      String topic = record.topic();
      int partition = record.kafkaPartition();
      TopicPartition tp = new TopicPartition(topic, partition);
      topicPartitionWriters.get(tp).buffer(record);
    }

    if (hiveIntegration) {
      Iterator<Future> iterator = hiveUpdateFutures.iterator();
      while (iterator.hasNext()) {
        try {
          Future future = iterator.next();
          if (future.isDone()) {
            future.get();
            iterator.remove();
          } else {
            break;
          }
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }

    for (TopicPartition tp: assignment) {
      topicPartitionWriters.get(tp).write();
    }
  }

  public void recover(TopicPartition tp) {
    topicPartitionWriters.get(tp).recover();
  }

  public void syncWithHive() throws ConnectException {
    Set<String> topics = new HashSet<>();
    for (TopicPartition tp: assignment) {
      topics.add(tp.topic());
    }

    try {
      for (String topic: topics) {
        String topicDir = FileUtils.topicDirectory(url, topicsDir, topic);
        CommittedFileFilter filter = new TopicCommittedFileFilter(topic);
        FileStatus fileStatusWithMaxOffset = FileUtils.fileStatusWithMaxOffset(storage, new Path(topicDir), filter);
        if (fileStatusWithMaxOffset != null) {
          Schema latestSchema = schemaFileReader.getSchema(conf, fileStatusWithMaxOffset.getPath());
          hive.createTable(hiveDatabase, topic, latestSchema, partitioner);
          List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase, topic, (short) -1);
          FileStatus[] statuses = FileUtils.getDirectories(storage, new Path(topicDir));
          for (FileStatus status : statuses) {
            String location = status.getPath().toString();
            if (!partitions.contains(location)) {
              String partitionValue = getPartitionValue(location);
              hiveMetaStore.addPartition(hiveDatabase, topic, partitionValue);
            }
          }
        }
      }
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  public void open(Collection<TopicPartition> partitions) {
    assignment = new HashSet<>(partitions);
    for (TopicPartition tp: assignment) {
      TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
          tp, storage, writerProvider, partitioner, connectorConfig, context, avroData,
          hiveMetaStore, hive, schemaFileReader, executorService, hiveUpdateFutures);
      topicPartitionWriters.put(tp, topicPartitionWriter);
      // We need to immediately start recovery to ensure we pause consumption of messages for the
      // assigned topics while we try to recover offsets and rewind.
      recover(tp);
    }
  }

  public void close(Collection<TopicPartition> partitions) {
    // Close any writers we have. We may get assigned the same partitions and end up duplicating
    // some effort since we'll have to reprocess those messages. It may be possible to hold on to
    // the TopicPartitionWriter and continue to use the temp file, but this can get significantly
    // more complex due to potential failures and network partitions. For example, we may get
    // this close, then miss a few generations of group membership, during which
    // data may have continued to be processed and we'd have to restart from the recovery stage,
    // make sure we apply the WAL, and only reuse the temp file if the starting offset is still
    // valid. For now, we prefer the simpler solution that may result in a bit of wasted effort.
    for (TopicPartition tp: assignment) {
      try {
        topicPartitionWriters.get(tp).close();
      } catch (ConnectException e) {
        log.error("Error closing writer for {}. Error: {]", tp, e.getMessage());
      } finally {
        topicPartitionWriters.remove(tp);
      }
    }
  }

  public void stop() {
    if (executorService != null) {
      boolean terminated = false;
      try {
        log.info("Shutting down Hive executor service.");
        executorService.shutdown();
        long shutDownTimeout = connectorConfig.getLong(HdfsSinkConnectorConfig.SHUTDOWN_TIMEOUT_CONFIG);
        log.info("Awaiting termination.");
        terminated = executorService.awaitTermination(shutDownTimeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // ignored
      }

      if (!terminated) {
        log.warn("Unclean Hive executor service shutdown, "
                 + "you probably need to sync with Hive next time you start the connector");
        executorService.shutdownNow();
      }
    }

    try {
      storage.close();
    } catch (IOException e) {
      throw new ConnectException(e);
    }
    if (ticketRenewThread != null) {
      synchronized (this) {
        isRunning = false;
        this.notifyAll();
      }
    }
  }

  public Partitioner getPartitioner() {
    return partitioner;
  }

  public Map<TopicPartition, Long> getCommittedOffsets() {
    for (TopicPartition tp: assignment) {
      offsets.put(tp, topicPartitionWriters.get(tp).offset());
    }
    return offsets;
  }

  public TopicPartitionWriter getBucketWriter(TopicPartition tp) {
    return topicPartitionWriters.get(tp);
  }

  public Storage getStorage() {
    return storage;
  }

  public Map<String, RecordWriter> getWriters(TopicPartition tp) {
    return topicPartitionWriters.get(tp).getWriters();
  }

  public Map<String, String> getTempFileNames(TopicPartition tp) {
    TopicPartitionWriter topicPartitionWriter = topicPartitionWriters.get(tp);
    return topicPartitionWriter.getTempFiles();
  }

  private void createDir(String dir) throws IOException {
    String path = url + "/" + dir;
    if (!storage.exists(path)) {
      storage.mkdirs(path);
    }
  }

  @SuppressWarnings("unchecked")
  private Format getFormat() throws ClassNotFoundException, IllegalAccessException, InstantiationException{
    return  ((Class<Format>) Class.forName(connectorConfig.getString(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG))).newInstance();
  }

  private String getPartitionValue(String path) {
    String[] parts = path.split("/");
    StringBuilder sb =  new StringBuilder();
    sb.append("/");
    for (int i = 3; i < parts.length; ++i) {
      sb.append(parts[i]);
      sb.append("/");
    }
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  private Partitioner createPartitioner(HdfsSinkConnectorConfig config)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    Class<? extends Partitioner> partitionerClasss = (Class<? extends Partitioner>)
        Class.forName(config.getString(HdfsSinkConnectorConfig.PARTITIONER_CLASS_CONFIG));

    Map<String, Object> map = copyConfig(config);
    Partitioner partitioner = partitionerClasss.newInstance();
    partitioner.configure(map);
    return partitioner;
  }

  private Map<String, Object> copyConfig(HdfsSinkConnectorConfig config) {
    Map<String, Object> map = new HashMap<>();
    map.put(HdfsSinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG, config.getString(HdfsSinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG));
    map.put(HdfsSinkConnectorConfig.PARTITION_DURATION_MS_CONFIG, config.getLong(HdfsSinkConnectorConfig.PARTITION_DURATION_MS_CONFIG));
    map.put(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG, config.getString(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG));
    map.put(HdfsSinkConnectorConfig.LOCALE_CONFIG, config.getString(HdfsSinkConnectorConfig.LOCALE_CONFIG));
    map.put(HdfsSinkConnectorConfig.TIMEZONE_CONFIG, config.getString(HdfsSinkConnectorConfig.TIMEZONE_CONFIG));
    return map;
  }
}
