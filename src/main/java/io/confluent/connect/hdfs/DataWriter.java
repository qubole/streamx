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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  private Set<TopicPartition> lastAssignment;
  private HdfsSinkConnectorConfig connectorConfig;
  private AvroData avroData;
  private SinkTaskContext context;
  private ExecutorService executorService;
  private String hiveDatabase;
  private HiveMetaStore hiveMetaStore;
  private HiveUtil hive;
  private Queue<Future> hiveUpdateFutures;
  private boolean hiveIntegration;

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
      conf.addResource(new Path(hadoopConfDir + "/core-site.xml"));
      conf.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"));
      log.info(conf.toString());

      url = connectorConfig.getString(HdfsSinkConnectorConfig.HDFS_URL_CONFIG);
      topicsDir = connectorConfig.getString(HdfsSinkConnectorConfig.TOPICS_DIR_CONFIG);
      String logsDir = connectorConfig.getString(HdfsSinkConnectorConfig.LOGS_DIR_CONFIG);

      Class<? extends Storage> storageClass = (Class<? extends Storage>) Class
              .forName(connectorConfig.getString(HdfsSinkConnectorConfig.STORAGE_CLASS_CONFIG));
      storage = StorageFactory.createStorage(storageClass, conf, url);

      createDir(topicsDir);
      createDir(logsDir);

      format = getFormat();
      writerProvider = format.getRecordWriterProvider();
      schemaFileReader = format.getSchemaFileReader(avroData);

      partitioner = createPartitioner(connectorConfig);

      assignment = new HashSet<>(context.assignment());
      offsets = new HashMap<>();
      lastAssignment = new HashSet<>();

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

  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    assignment = new HashSet<>(partitions);

    // handle partitions that no longer assigned to the task
    for (TopicPartition tp: lastAssignment) {
      if (!assignment.contains(tp)) {
        try {
          if (topicPartitionWriters.containsKey(tp)) {
            topicPartitionWriters.get(tp).close();
          }
        } catch (ConnectException e) {
          log.error("Error closing writer for {}. Error: {]", tp, e.getMessage());
        } finally {
          topicPartitionWriters.remove(tp);
        }
      }
    }

    // handle new partitions
    for (TopicPartition tp: assignment) {
      if (!lastAssignment.contains(tp)) {
        TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
            tp, storage, writerProvider, partitioner, connectorConfig, context, avroData, hiveMetaStore, hive, schemaFileReader, executorService,
            hiveUpdateFutures);
        topicPartitionWriters.put(tp, topicPartitionWriter);
      }
    }
  }

  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    lastAssignment = new HashSet<>(partitions);
  }

  public void close() {
    for (TopicPartition tp: assignment) {
      topicPartitionWriters.get(tp).close();
    }

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
    return  ((Class<Format>) Class.forName(connectorConfig.getString(HdfsSinkConnectorConfig.FORMAT_CONFIG))).newInstance();
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
    map.put(HdfsSinkConnectorConfig.PARTITION_FIELD_CONFIG, config.getString(HdfsSinkConnectorConfig.PARTITION_FIELD_CONFIG));
    map.put(HdfsSinkConnectorConfig.PARTITION_DURATION_CONFIG, config.getLong(HdfsSinkConnectorConfig.PARTITION_DURATION_CONFIG));
    map.put(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG, config.getString(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG));
    map.put(HdfsSinkConnectorConfig.LOCALE_CONFIG, config.getString(HdfsSinkConnectorConfig.LOCALE_CONFIG));
    map.put(HdfsSinkConnectorConfig.TIMEZONE_CONFIG, config.getString(HdfsSinkConnectorConfig.TIMEZONE_CONFIG));
    return map;
  }
}
