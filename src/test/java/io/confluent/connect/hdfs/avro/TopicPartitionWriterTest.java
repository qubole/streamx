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

package io.confluent.connect.hdfs.avro;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.Format;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.SchemaFileReader;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.hdfs.TopicPartitionWriter;
import io.confluent.connect.hdfs.filter.CommittedFileFilter;
import io.confluent.connect.hdfs.partitioner.DefaultPartitioner;
import io.confluent.connect.hdfs.partitioner.FieldPartitioner;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.hdfs.partitioner.TimeBasedPartitioner;
import io.confluent.connect.hdfs.partitioner.TimeUtils;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.hdfs.storage.StorageFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TopicPartitionWriterTest extends TestWithMiniDFSCluster {
  // The default based on default configuration of 10
  private static final String ZERO_PAD_FMT = "%010d";

  private RecordWriterProvider writerProvider;
  private SchemaFileReader schemaFileReader;
  private Storage storage;
  private static String extension;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    super.setUp();

    Format format = ((Class<Format>) Class.forName(connectorConfig.getString(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG))).newInstance();
    writerProvider = format.getRecordWriterProvider();
    schemaFileReader = format.getSchemaFileReader(avroData);
    extension = writerProvider.getExtension();
    Class<? extends Storage> storageClass = (Class<? extends Storage>) Class
            .forName(connectorConfig.getString(HdfsSinkConnectorConfig.STORAGE_CLASS_CONFIG));
    storage = StorageFactory.createStorage(storageClass, conf, url);
    createTopicDir(url, topicsDir, TOPIC);
    createLogsDir(url, logsDir);
  }

  @Test
  public void testWriteRecordDefaultWithPadding() throws Exception {
    Partitioner partitioner = new DefaultPartitioner();
    partitioner.configure(Collections.<String, Object>emptyMap());
    connectorProps.put(HdfsSinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, "2");
    configureConnector();
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner,  connectorConfig, context, avroData);

    String key = "key";
    Schema schema = createSchema();
    Struct[] records = createRecords(schema);

    Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.recover();
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    Set<Path> expectedFiles = new HashSet<>();
    expectedFiles.add(new Path(url + "/" + topicsDir + "/" + TOPIC + "/partition=" + PARTITION +
                               "/" + TOPIC + "+" + PARTITION + "+00+02" + extension));
    expectedFiles.add(new Path(url + "/" + topicsDir + "/" + TOPIC + "/partition=" + PARTITION +
                               "/" + TOPIC + "+" + PARTITION + "+03+05" + extension));
    expectedFiles.add(new Path(url + "/" + topicsDir + "/" + TOPIC + "/partition=" + PARTITION +
                               "/" + TOPIC + "+" + PARTITION + "+06+08" + extension));
    verify(expectedFiles, records, schema);
  }


  @Test
  public void testWriteRecordFieldPartitioner() throws Exception {
    Map<String, Object> config = createConfig();
    Partitioner partitioner = new FieldPartitioner();
    partitioner.configure(config);

    String partitionField = (String) config.get(HdfsSinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, avroData);

    String key = "key";
    Schema schema = createSchema();
    Struct[] records = createRecords(schema);

    Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.recover();
    topicPartitionWriter.write();
    topicPartitionWriter.close();


    String directory1 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(16));
    String directory2 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(17));
    String directory3 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(18));

    Set<Path> expectedFiles = new HashSet<>();
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory1, TOPIC_PARTITION, 0, 2, extension, ZERO_PAD_FMT)));
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory2, TOPIC_PARTITION, 3, 5, extension, ZERO_PAD_FMT)));
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory3, TOPIC_PARTITION, 6, 8, extension, ZERO_PAD_FMT)));

    verify(expectedFiles, records, schema);
  }

  @Test
  public void testWriteRecordTimeBasedPartition() throws Exception {
    Map<String, Object> config = createConfig();
    Partitioner partitioner = new TimeBasedPartitioner();
    partitioner.configure(config);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, avroData);

    String key = "key";
    Schema schema = createSchema();
    Struct[] records = createRecords(schema);

    Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.recover();
    topicPartitionWriter.write();
    topicPartitionWriter.close();


    long partitionDurationMs = (Long) config.get(HdfsSinkConnectorConfig.PARTITION_DURATION_MS_CONFIG);
    String pathFormat = (String) config.get(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG);
    String timeZoneString = (String) config.get(HdfsSinkConnectorConfig.TIMEZONE_CONFIG);
    long timestamp = System.currentTimeMillis();

    String encodedPartition = TimeUtils.encodeTimestamp(partitionDurationMs, pathFormat, timeZoneString, timestamp);

    String directory = partitioner.generatePartitionedPath(TOPIC, encodedPartition);

    Set<Path> expectedFiles = new HashSet<>();
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory, TOPIC_PARTITION, 0, 2, extension, ZERO_PAD_FMT)));
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory, TOPIC_PARTITION, 3, 5, extension, ZERO_PAD_FMT)));
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory, TOPIC_PARTITION, 6, 8, extension, ZERO_PAD_FMT)));

    verify(expectedFiles, records, schema);
  }

  private Map<String, Object> createConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put(HdfsSinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG, "int");
    config.put(HdfsSinkConnectorConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.HOURS.toMillis(1));
    config.put(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG, "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/");
    config.put(HdfsSinkConnectorConfig.LOCALE_CONFIG, "en");
    config.put(HdfsSinkConnectorConfig.TIMEZONE_CONFIG, "America/Los_Angeles");
    return config;
  }

  private void createTopicDir(String url, String topicsDir, String topic) throws IOException {
    Path path = new Path(FileUtils.topicDirectory(url, topicsDir, topic));
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
  }

  private void createLogsDir(String url, String logsDir) throws IOException {
    Path path = new Path(url + "/" + logsDir);
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
  }

  private Struct[] createRecords(Schema schema) {
    Struct record1 = new Struct(schema)
        .put("boolean", true)
        .put("int", 16)
        .put("long", 12L)
        .put("float", 12.2f)
        .put("double", 12.2);

    Struct record2 = new Struct(schema)
        .put("boolean", true)
        .put("int", 17)
        .put("long", 12L)
        .put("float", 12.2f)
        .put("double", 12.2);

    Struct record3 = new Struct(schema)
        .put("boolean", true)
        .put("int", 18)
        .put("long", 12L)
        .put("float", 12.2f)
        .put("double", 12.2);

    ArrayList<Struct> records = new ArrayList<>();
    records.add(record1);
    records.add(record2);
    records.add(record3);
    return records.toArray(new Struct[records.size()]);
  }


  private ArrayList<SinkRecord> createSinkRecords(Struct[] records, String key, Schema schema) {
    ArrayList<SinkRecord> sinkRecords = new ArrayList<>();
    long offset = 0;
    for (Struct record : records) {
      for (long count = 0; count < 3; count++) {
        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record,
                                               offset + count);
        sinkRecords.add(sinkRecord);
      }
      offset = offset + 3;
    }
    return sinkRecords;
  }

  private void verify(Set<Path> expectedFiles, Struct[] records, Schema schema) throws IOException {
    Path path = new Path(FileUtils.topicDirectory(url, topicsDir, TOPIC));
    FileStatus[] statuses = FileUtils.traverse(storage, path, new CommittedFileFilter());
    assertEquals(expectedFiles.size(), statuses.length);
    int index = 0;
    for (FileStatus status : statuses) {
      Path filePath = status.getPath();
      assertTrue(expectedFiles.contains(status.getPath()));
      Collection<Object> avroRecords = schemaFileReader.readData(conf, filePath);
      assertEquals(3, avroRecords.size());
      for (Object avroRecord: avroRecords) {
        assertEquals(avroData.fromConnectData(schema, records[index]), avroRecord);
      }
      index++;
    }
  }
}
