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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.SchemaFileReader;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.hdfs.filter.TopicPartitionCommittedFileFilter;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.hdfs.storage.StorageFactory;
import io.confluent.connect.hdfs.wal.WAL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataWriterAvroTest extends TestWithMiniDFSCluster {

  private static final String extension = ".avro";
  private static final String ZERO_PAD_FMT = "%010d";
  private SchemaFileReader schemaFileReader = new AvroFileReader(avroData);
  
  @Test
  public void testWriteRecord() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    Partitioner partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = 0; offset < 7; offset++) {
      SinkRecord sinkRecord =
          new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset);

      sinkRecords.add(sinkRecord);
    }
    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();

    String encodedPartition = "partition=" + String.valueOf(PARTITION);
    String directory = partitioner.generatePartitionedPath(TOPIC, encodedPartition);

    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {-1, 2, 5};
    for (int i = 1; i < validOffsets.length; i++) {
      long startOffset = validOffsets[i - 1] + 1;
      long endOffset = validOffsets[i];
      Path path =
          new Path(FileUtils
                       .committedFileName(url, topicsDir, directory, TOPIC_PARTITION, startOffset,
                                          endOffset, extension, ZERO_PAD_FMT));
      Collection<Object> records = schemaFileReader.readData(conf, path);
      long size = endOffset - startOffset + 1;
      assertEquals(size, records.size());
      for (Object avroRecord : records) {
        assertEquals(avroData.fromConnectData(schema, record), avroRecord);
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRecovery() throws Exception {
    fs.delete(new Path(FileUtils.directoryName(url, topicsDir, TOPIC_PARTITION)), true);

    Class<? extends Storage> storageClass = (Class<? extends Storage>)
        Class.forName(connectorConfig.getString(HdfsSinkConnectorConfig.STORAGE_CLASS_CONFIG));
    Storage storage = StorageFactory.createStorage(storageClass, conf, url);

    WAL wal = storage.wal(logsDir, TOPIC_PARTITION);

    wal.append(WAL.beginMarker, "");
    Set<String> committedFiles = new HashSet<>();

    String directory = TOPIC + "/" + "partition=" + String.valueOf(PARTITION);

    for (int i = 0; i < 5; ++i) {
      long startOffset = i * 10;
      long endOffset = (i + 1) * 10 - 1;
      String tempfile = FileUtils.tempFileName(url, topicsDir, directory, extension);
      fs.createNewFile(new Path(tempfile));
      String committedFile = FileUtils.committedFileName(url, topicsDir, directory, TOPIC_PARTITION, startOffset,
                                                         endOffset, extension, ZERO_PAD_FMT);
      committedFiles.add(committedFile);
      wal.append(tempfile, committedFile);
    }
    wal.append(WAL.endMarker, "");
    wal.close();

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    hdfsWriter.recover(TOPIC_PARTITION);
    Map<TopicPartition, Long> offsets = context.offsets();
    assertTrue(offsets.containsKey(TOPIC_PARTITION));
    assertEquals(50L, (long) offsets.get(TOPIC_PARTITION));

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    // Need enough records to trigger file rotation
    ArrayList<SinkRecord> sinkRecords = new ArrayList<>();
    for (int i = 0; i < 3; i++)
      sinkRecords.add(
          new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 50 + i));

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();

    committedFiles.add(FileUtils.committedFileName(url, topicsDir, directory, TOPIC_PARTITION,
                                                   50, 52, extension, ZERO_PAD_FMT));
    FileStatus[] statuses = fs.listStatus(new Path(FileUtils.directoryName(url, topicsDir, directory)),
                      new TopicPartitionCommittedFileFilter(TOPIC_PARTITION));
    assertEquals(committedFiles.size(), statuses.length);
    for (FileStatus status : statuses) {
      assertTrue(committedFiles.contains(status.getPath().toString()));
    }
  }

  @Test
  public void testWriteRecordMultiplePartitions() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);

    for (TopicPartition tp: assignment) {
      hdfsWriter.recover(tp);
    }
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp: assignment) {
      for (long offset = 0; offset < 7; offset++) {
        SinkRecord sinkRecord =
            new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset);
        sinkRecords.add(sinkRecord);
      }
    }
    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();

    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {-1, 2, 5};

    for (TopicPartition tp : assignment) {
      String directory = tp.topic() + "/" + "partition=" + String.valueOf(tp.partition());
      for (int j = 1; j < validOffsets.length; ++j) {
        long startOffset = validOffsets[j - 1] + 1;
        long endOffset = validOffsets[j];
        Path path = new Path(
            FileUtils.committedFileName(url, topicsDir, directory, tp, startOffset, endOffset,
                                        extension, ZERO_PAD_FMT));
        Collection<Object> records = schemaFileReader.readData(conf, path);
        long size = endOffset - startOffset + 1;
        assertEquals(records.size(), size);
        for (Object avroRecord : records) {
          assertEquals(avroRecord, avroData.fromConnectData(schema, record));
        }
      }
    }
  }

  @Test
  public void testGetPreviousOffsets() throws Exception {
    String directory = TOPIC + "/" + "partition=" + String.valueOf(PARTITION);
    long[] startOffsets = {0, 3};
    long[] endOffsets = {2, 5};

    for (int i = 0; i < startOffsets.length; ++i) {
      Path path = new Path(FileUtils.committedFileName(url, topicsDir, directory, TOPIC_PARTITION, startOffsets[i],
                                                       endOffsets[i], extension, ZERO_PAD_FMT));
      fs.createNewFile(path);
    }
    Path path = new Path(FileUtils.tempFileName(url, topicsDir, directory, extension));
    fs.createNewFile(path);

    path = new Path(FileUtils.fileName(url, topicsDir, directory, "abcd"));
    fs.createNewFile(path);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    hdfsWriter.recover(TOPIC_PARTITION);

    Map<TopicPartition, Long> committedOffsets = hdfsWriter.getCommittedOffsets();

    assertTrue(committedOffsets.containsKey(TOPIC_PARTITION));
    long previousOffset = committedOffsets.get(TOPIC_PARTITION);
    assertEquals(previousOffset, 6L);

    hdfsWriter.close();
  }

  @Test
  public void testWriteRecordNonZeroInitailOffset() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    Partitioner partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = 3; offset < 10; offset++) {
      SinkRecord sinkRecord =
          new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset);
      sinkRecords.add(sinkRecord);
    }

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();

    String directory = partitioner.generatePartitionedPath(TOPIC, "partition=" + String.valueOf(PARTITION));

    // Last file (offset 9) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {2, 5, 8};
    for (int i = 1; i < validOffsets.length; i++) {
      long startOffset = validOffsets[i - 1] + 1;
      long endOffset = validOffsets[i];
      Path path = new Path(FileUtils.committedFileName(url, topicsDir, directory,
                                                       TOPIC_PARTITION, startOffset, endOffset,
                                                       extension, ZERO_PAD_FMT));
      Collection<Object> records = schemaFileReader.readData(conf, path);
      long size = endOffset - startOffset + 1;
      assertEquals(size, records.size());
      for (Object avroRecord : records) {
        assertEquals(avroData.fromConnectData(schema, record), avroRecord);
      }
    }
  }

  @Test
  public void testRebalance() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);

    // Initial assignment is {TP1, TP2}
    for (TopicPartition tp: assignment) {
      hdfsWriter.recover(tp);
    }
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp: assignment) {
      for (long offset = 0; offset < 7; offset++) {
        SinkRecord sinkRecord =
            new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset);
        sinkRecords.add(sinkRecord);
      }
    }
    hdfsWriter.write(sinkRecords);

    Set<TopicPartition> oldAssignment = new HashSet<>(assignment);

    Set<TopicPartition> newAssignment = new HashSet<>();
    newAssignment.add(TOPIC_PARTITION);
    newAssignment.add(TOPIC_PARTITION3);
    hdfsWriter.onPartitionsRevoked(assignment);
    assignment = newAssignment;
    hdfsWriter.onPartitionsAssigned(newAssignment);

    assertEquals(null, hdfsWriter.getBucketWriter(TOPIC_PARTITION2));
    assertNotNull(hdfsWriter.getBucketWriter(TOPIC_PARTITION));
    assertNotNull(hdfsWriter.getBucketWriter(TOPIC_PARTITION3));

    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsetsTopicPartition2 = {-1, 2, 5};
    String directory2 = TOPIC + "/" + "partition=" + String.valueOf(PARTITION2);
    for (int j = 1; j < validOffsetsTopicPartition2.length; ++j) {
      long startOffset = validOffsetsTopicPartition2[j - 1] + 1;
      long endOffset = validOffsetsTopicPartition2[j];
      Path path = new Path(FileUtils.committedFileName(url, topicsDir, directory2,
                                                       TOPIC_PARTITION2, startOffset, endOffset,
                                                       extension, ZERO_PAD_FMT));
      Collection<Object> records = schemaFileReader.readData(conf, path);
      long size = endOffset - startOffset + 1;
      assertEquals(records.size(), size);
      for (Object avroRecord : records) {
        assertEquals(avroData.fromConnectData(schema, record), avroRecord);
      }
    }

    sinkRecords.clear();
    for (TopicPartition tp: assignment) {
      // Message offsets start at 6 because we discarded the in-progress temp file on rebalance
      for (long offset = 6; offset < 10; offset++) {
        SinkRecord sinkRecord =
            new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset);
        sinkRecords.add(sinkRecord);
      }
    }

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();

    // Last file (offset 9) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsetsTopicPartition1 = {5, 8};
    String directory1 = TOPIC + "/" + "partition=" + String.valueOf(PARTITION);
    for (int j = 1; j < validOffsetsTopicPartition1.length; ++j) {
      long startOffset = validOffsetsTopicPartition1[j - 1] + 1;
      long endOffset = validOffsetsTopicPartition1[j];
      Path path = new Path(FileUtils.committedFileName(url, topicsDir, directory1 ,
                                                       TOPIC_PARTITION, startOffset, endOffset,
                                                       extension, ZERO_PAD_FMT));
      Collection<Object> records = schemaFileReader.readData(conf, path);
      long size = endOffset - startOffset + 1;
      assertEquals(records.size(), size);
      for (Object avroRecord : records) {
        assertEquals(avroData.fromConnectData(schema, record), avroRecord);
      }
    }

    long[] validOffsetsTopicPartition3 = {5, 8};
    String directory3 = TOPIC + "/" + "partition=" + String.valueOf(PARTITION3);
    for (int j = 1; j < validOffsetsTopicPartition3.length; ++j) {
      long startOffset = validOffsetsTopicPartition3[j - 1] + 1;
      long endOffset = validOffsetsTopicPartition3[j];
      Path path = new Path(FileUtils.committedFileName(url, topicsDir, directory3,
                                                       TOPIC_PARTITION3, startOffset, endOffset,
                                                       extension, ZERO_PAD_FMT));
      Collection<Object> records = schemaFileReader.readData(conf, path);
      long size = endOffset - startOffset + 1;
      assertEquals(records.size(), size);
      for (Object avroRecord : records) {
        assertEquals(avroData.fromConnectData(schema, record), avroRecord);
      }
    }
    assignment = oldAssignment;
  }


  @Test
  public void testProjectBackWard() throws Exception {
    Map<String, String> props = createProps();
    props.put(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    props.put(HdfsSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    hdfsWriter.recover(TOPIC_PARTITION);

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Schema newSchema = createNewSchema();
    Struct newRecord = createNewRecord(newSchema);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, newSchema, newRecord, 0L));
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1L));

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();

    String DIRECTORY = TOPIC + "/" + "partition=" + String.valueOf(PARTITION);
    Path path = new Path(FileUtils.committedFileName(url, topicsDir, DIRECTORY, TOPIC_PARTITION,
                                                     0L, 1L, extension, ZERO_PAD_FMT));
    ArrayList<Object> records = (ArrayList<Object>) schemaFileReader.readData(conf, path);
    assertEquals(2, records.size());

    assertEquals(avroData.fromConnectData(newSchema, newRecord), records.get(0));

    Object projected = SchemaProjector.project(schema, record, newSchema);
    assertEquals(avroData.fromConnectData(newSchema, projected), records.get(1));

    hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    hdfsWriter.recover(TOPIC_PARTITION);

    sinkRecords.clear();
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 2L));
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, newSchema,
                                   newRecord, 3L));

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();

    path = new Path(FileUtils.committedFileName(url, topicsDir, DIRECTORY, TOPIC_PARTITION, 2L,
                                                3L, extension, ZERO_PAD_FMT));
    records = (ArrayList<Object>) schemaFileReader.readData(conf, path);
    assertEquals(2, records.size());

    assertEquals(avroData.fromConnectData(newSchema, projected), records.get(0));
    assertEquals(avroData.fromConnectData(newSchema, newRecord), records.get(1));
  }

  @Test
  public void testProjectNone() throws Exception {
    Map<String, String> props = createProps();
    props.put(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    hdfsWriter.recover(TOPIC_PARTITION);

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Schema newSchema = createNewSchema();
    Struct newRecord = createNewRecord(newSchema);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, newSchema, newRecord, 0L));
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1L));
    // Include one more to get to forced file rotation
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 2L));

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();

    String DIRECTORY = TOPIC + "/" + "partition=" + String.valueOf(PARTITION);
    Path path = new Path(FileUtils.committedFileName(url, topicsDir, DIRECTORY, TOPIC_PARTITION,
                                                     0L, 0L, extension, ZERO_PAD_FMT));
    ArrayList<Object> records = (ArrayList<Object>) schemaFileReader.readData(conf, path);
    assertEquals(1, records.size());


    assertEquals(avroData.fromConnectData(newSchema, newRecord), records.get(0));

    path = new Path(FileUtils.committedFileName(url, topicsDir, DIRECTORY, TOPIC_PARTITION, 1L,
                                                2L, extension, ZERO_PAD_FMT));
    records = (ArrayList<Object>) schemaFileReader.readData(conf, path);
    assertEquals(avroData.fromConnectData(schema, record), records.get(0));
    assertEquals(avroData.fromConnectData(schema, record), records.get(1));

    hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    hdfsWriter.recover(TOPIC_PARTITION);

    sinkRecords.clear();
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, newSchema, newRecord, 3L));
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, newSchema, newRecord, 4L));

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();

    path = new Path(FileUtils.committedFileName(url, topicsDir, DIRECTORY, TOPIC_PARTITION, 3L,
                                                4L, extension, ZERO_PAD_FMT));
    records = (ArrayList<Object>) schemaFileReader.readData(conf, path);
    assertEquals(2, records.size());

    assertEquals(avroData.fromConnectData(newSchema, newRecord), records.get(0));
    assertEquals(avroData.fromConnectData(newSchema, newRecord), records.get(1));
  }

  @Test
  public void testProjectForward() throws Exception {
    Map<String, String> props = createProps();
    props.put(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    props.put(HdfsSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "FORWARD");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    hdfsWriter.recover(TOPIC_PARTITION);

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Schema newSchema = createNewSchema();
    Struct newRecord = createNewRecord(newSchema);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, newSchema, newRecord, 0L));
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1L));
    // Include one more to get to forced file rotation
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 2L));

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();

    String DIRECTORY = TOPIC + "/" + "partition=" + String.valueOf(PARTITION);
    Path path = new Path(FileUtils.committedFileName(url, topicsDir, DIRECTORY, TOPIC_PARTITION,
                                                     0L, 0L, extension, ZERO_PAD_FMT));
    ArrayList<Object> records = (ArrayList<Object>) schemaFileReader.readData(conf, path);
    assertEquals(1, records.size());

    assertEquals(avroData.fromConnectData(newSchema, newRecord), records.get(0));

    path = new Path(FileUtils.committedFileName(url, topicsDir, DIRECTORY, TOPIC_PARTITION, 1L,
                                                2L, extension, ZERO_PAD_FMT));
    records = (ArrayList<Object>) schemaFileReader.readData(conf, path);
    assertEquals(avroData.fromConnectData(schema, record), records.get(0));
    assertEquals(avroData.fromConnectData(schema, record), records.get(1));

    hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    hdfsWriter.recover(TOPIC_PARTITION);

    sinkRecords.clear();
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, newSchema, newRecord, 3L));
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, newSchema, newRecord, 4L));

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();

    path = new Path(FileUtils.committedFileName(url, topicsDir, DIRECTORY, TOPIC_PARTITION, 3L,
                                                4L, extension, ZERO_PAD_FMT));
    records = (ArrayList<Object>) schemaFileReader.readData(conf, path);
    assertEquals(2, records.size());

    assertEquals(avroData.fromConnectData(schema, record), records.get(0));
    assertEquals(avroData.fromConnectData(schema, record), records.get(1));
  }

  @Test
  public void testProjectNoVersion() throws Exception {
    Schema schemaNoVersion = SchemaBuilder.struct().name("record")
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("int", Schema.INT32_SCHEMA)
        .field("long", Schema.INT64_SCHEMA)
        .field("float", Schema.FLOAT32_SCHEMA)
        .field("double", Schema.FLOAT64_SCHEMA)
        .build();

    Struct recordNoVersion = new Struct(schemaNoVersion);
    recordNoVersion.put("boolean", true)
        .put("int", 12)
        .put("long", 12L)
        .put("float", 12.2f)
        .put("double", 12.2);

    Map<String, String> props = createProps();
    props.put(HdfsSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    hdfsWriter.recover(TOPIC_PARTITION);

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schemaNoVersion, recordNoVersion, 0L));
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1L));

    try {
      hdfsWriter.write(sinkRecords);
      fail("Version is required for Backward compatibility.");
    } catch (RuntimeException e) {
      // expected
    }

    hdfsWriter.close();
  }
}

