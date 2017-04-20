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

import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.SchemaFileReader;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import io.confluent.connect.hdfs.filter.TopicPartitionCommittedFileFilter;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.hdfs.storage.StorageFactory;
import io.confluent.connect.hdfs.wal.WAL;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataWriterAvroTest extends TestWithMiniDFSCluster {

  private static final String extension = ".avro";
  private static final String ZERO_PAD_FMT = "%010d";
  private SchemaFileReader schemaFileReader = new AvroFileReader(avroData);
  Partitioner partitioner;

  @Test
  public void testWriteRecord() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createRecords(7);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close(assignment);
    hdfsWriter.stop();

    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testRecovery() throws Exception {
    fs.delete(new Path(FileUtils.directoryName(url, topicsDir, TOPIC_PARTITION)), true);

    @SuppressWarnings("unchecked")
    Class<? extends Storage> storageClass = (Class<? extends Storage>)
        Class.forName(connectorConfig.getString(HdfsSinkConnectorConfig.STORAGE_CLASS_CONFIG));
    Storage storage = StorageFactory.createStorage(storageClass, conf, url);
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();

    WAL wal = storage.wal(logsDir, TOPIC_PARTITION);

    wal.append(WAL.beginMarker, "");

    for (int i = 0; i < 5; ++i) {
      long startOffset = i * 10;
      long endOffset = (i + 1) * 10 - 1;
      String tempfile = FileUtils.tempFileName(url, topicsDir, getDirectory(), extension);
      fs.createNewFile(new Path(tempfile));
      String committedFile = FileUtils.committedFileName(url, topicsDir, getDirectory(), TOPIC_PARTITION, startOffset,
                                                         endOffset, extension, ZERO_PAD_FMT);
      wal.append(tempfile, committedFile);
    }
    wal.append(WAL.endMarker, "");
    wal.close();

    hdfsWriter.recover(TOPIC_PARTITION);
    Map<TopicPartition, Long> offsets = context.offsets();
    assertTrue(offsets.containsKey(TOPIC_PARTITION));
    assertEquals(50L, (long) offsets.get(TOPIC_PARTITION));

    List<SinkRecord> sinkRecords = createRecords(3, 50);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close(assignment);
    hdfsWriter.stop();

    long[] validOffsets = {0, 10, 20, 30, 40, 50, 53};
    verifyFileListing(validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
  }

  @Test
  public void testWriteRecordMultiplePartitions() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();

    for (TopicPartition tp: assignment) {
      hdfsWriter.recover(tp);
    }

    List<SinkRecord> sinkRecords = createRecords(7, 0, assignment);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close(assignment);
    hdfsWriter.stop();

    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, assignment);
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

    hdfsWriter.close(assignment);
    hdfsWriter.stop();
  }

  @Test
  public void testWriteRecordNonZeroInitialOffset() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createRecords(7, 3);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close(assignment);
    hdfsWriter.stop();

    // Last file (offset 9) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {3, 6, 9};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testRebalance() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();

    // Initial assignment is {TP1, TP2}
    for (TopicPartition tp: assignment) {
      hdfsWriter.recover(tp);
    }

    List<SinkRecord> sinkRecords = createRecords(7, 0, assignment);
    hdfsWriter.write(sinkRecords);

    Set<TopicPartition> oldAssignment = new HashSet<>(assignment);

    Set<TopicPartition> newAssignment = new HashSet<>();
    newAssignment.add(TOPIC_PARTITION);
    newAssignment.add(TOPIC_PARTITION3);

    hdfsWriter.close(assignment);
    assignment = newAssignment;
    hdfsWriter.open(newAssignment);

    assertEquals(null, hdfsWriter.getBucketWriter(TOPIC_PARTITION2));
    assertNotNull(hdfsWriter.getBucketWriter(TOPIC_PARTITION));
    assertNotNull(hdfsWriter.getBucketWriter(TOPIC_PARTITION3));

    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsetsTopicPartition2 = {0, 3, 6};
    verify(sinkRecords, validOffsetsTopicPartition2, Collections.singleton(TOPIC_PARTITION2), true);

    // Message offsets start at 6 because we discarded the in-progress temp file on re-balance
    sinkRecords = createRecords(3, 6, assignment);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close(newAssignment);
    hdfsWriter.stop();

    // Last file (offset 9) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsetsTopicPartition1 = {6, 9};
    verify(sinkRecords, validOffsetsTopicPartition1, Collections.singleton(TOPIC_PARTITION), true);

    long[] validOffsetsTopicPartition3 = {6, 9};
    verify(sinkRecords, validOffsetsTopicPartition3, Collections.singleton(TOPIC_PARTITION3), true);

    assignment = oldAssignment;
  }

  @Test
  public void testProjectBackWard() throws Exception {
    Map<String, String> props = createProps();
    props.put(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    props.put(HdfsSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createRecordsWithAlteringSchemas(7, 0);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close(assignment);
    hdfsWriter.stop();
    long[] validOffsets = {0, 1, 3, 5, 7};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testProjectNone() throws Exception {
    Map<String, String> props = createProps();
    props.put(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createRecordsWithAlteringSchemas(7, 0);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close(assignment);
    hdfsWriter.stop();

    long[] validOffsets = {0, 1, 2, 3, 4, 5, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testProjectForward() throws Exception {
    Map<String, String> props = createProps();
    props.put(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    props.put(HdfsSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "FORWARD");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    // By excluding the first element we get a list starting with record having the new schema.
    List<SinkRecord> sinkRecords = createRecordsWithAlteringSchemas(8, 0).subList(1, 8);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close(assignment);
    hdfsWriter.stop();

    long[] validOffsets = {1, 2, 4, 6, 8};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testProjectNoVersion() throws Exception {
    Map<String, String> props = createProps();
    props.put(HdfsSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createRecordsNoVersion(1, 0);
    sinkRecords.addAll(createRecordsWithAlteringSchemas(7, 0));

    try {
      hdfsWriter.write(sinkRecords);
      fail("Version is required for Backward compatibility.");
    } catch (RuntimeException e) {
      // expected
    } finally {
      hdfsWriter.close(assignment);
      hdfsWriter.stop();
      long[] validOffsets = {};
      verify(Collections.<SinkRecord>emptyList(), validOffsets);
    }
  }

  @Test
  public void testFlushPartialFile() throws Exception {
    String ROTATE_INTERVAL_MS_CONFIG = "1000";
    // wait for 2 * ROTATE_INTERVAL_MS_CONFIG
    long WAIT_TIME = Long.valueOf(ROTATE_INTERVAL_MS_CONFIG) * 2;

    String FLUSH_SIZE_CONFIG = "10";
    // send 1.5 * FLUSH_SIZE_CONFIG records
    int NUMBER_OF_RECORDS = Integer.valueOf(FLUSH_SIZE_CONFIG) + Integer.valueOf(FLUSH_SIZE_CONFIG) / 2;

    Map<String, String> props = createProps();
    props.put(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG, FLUSH_SIZE_CONFIG);
    props.put(HdfsSinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG, ROTATE_INTERVAL_MS_CONFIG);
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);
    assignment = new HashSet<>();
    assignment.add(TOPIC_PARTITION);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createRecords(NUMBER_OF_RECORDS);
    hdfsWriter.write(sinkRecords);

    // wait for rotation to happen
    long start = System.currentTimeMillis();
    long end = start + WAIT_TIME;
    while(System.currentTimeMillis() < end) {
      List<SinkRecord> messageBatch = new ArrayList<>();
      hdfsWriter.write(messageBatch);
    }

    Map<TopicPartition, Long> committedOffsets = hdfsWriter.getCommittedOffsets();
    assertTrue(committedOffsets.containsKey(TOPIC_PARTITION));
    long previousOffset = committedOffsets.get(TOPIC_PARTITION);
    assertEquals(NUMBER_OF_RECORDS, previousOffset);

    hdfsWriter.close(assignment);
    hdfsWriter.stop();
  }

  /**
   * Return a list of new records starting at zero offset.
   *
   * @param size the number of records to return.
   * @return the list of records.
   */
  protected List<SinkRecord> createRecords(int size) {
    return createRecords(size, 0);
  }

  /**
   * Return a list of new records starting at the given offset.
   *
   * @param size the number of records to return.
   * @param startOffset the starting offset.
   * @return the list of records.
   */
  protected List<SinkRecord> createRecords(int size, long startOffset) {
    return createRecords(size, startOffset, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
  }

  /**
   * Return a list of new records for a set of partitions, starting at the given offset in each partition.
   *
   * @param size the number of records to return.
   * @param startOffset the starting offset.
   * @param partitions the set of partitions to create records for.
   * @return the list of records.
   */
  protected List<SinkRecord> createRecords(int size, long startOffset, Set<TopicPartition> partitions) {
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      for (long offset = startOffset; offset < startOffset + size; ++offset) {
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
      }
    }
    return sinkRecords;
  }

  protected List<SinkRecord> createRecordsNoVersion(int size, long startOffset) {
    String key = "key";
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

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = startOffset; offset < startOffset + size; ++offset) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schemaNoVersion,
                                     recordNoVersion, offset));
    }
    return sinkRecords;
  }

  protected List<SinkRecord> createRecordsWithAlteringSchemas(int size, long startOffset) {
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);
    Schema newSchema = createNewSchema();
    Struct newRecord = createNewRecord(newSchema);

    int limit = (size / 2) * 2;
    boolean remainder = size % 2 > 0;
    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = startOffset; offset < startOffset + limit; ++offset) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset));
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, newSchema, newRecord, ++offset));
    }
    if (remainder) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record,
                                     startOffset + size - 1));
    }
    return sinkRecords;
  }

  protected List<SinkRecord> createRecordsInterleaved(int size, long startOffset, Set<TopicPartition> partitions) {
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = startOffset, total = 0; total < size; ++offset) {
      for (TopicPartition tp : partitions) {
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
        if (++total >= size) {
          break;
        }
      }
    }
    return sinkRecords;
  }

  protected String getDirectory() {
    return getDirectory(TOPIC, PARTITION);
  }

  protected String getDirectory(String topic, int partition) {
    String encodedPartition = "partition=" + String.valueOf(partition);
    return partitioner.generatePartitionedPath(topic, encodedPartition);
  }

  /**
   * Verify files and records are uploaded appropriately.
   * @param sinkRecords a flat list of the records that need to appear in potentially several files in S3.
   * @param validOffsets an array containing the offsets that map to uploaded files for a topic-partition.
   *                     Offsets appear in ascending order, the difference between two consecutive offsets
   *                     equals the expected size of the file, and last offset is exclusive.
   */
  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets) throws IOException {
    verify(sinkRecords, validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)), false);
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions)
      throws IOException {
    verify(sinkRecords, validOffsets, partitions, false);
  }

  /**
   * Verify files and records are uploaded appropriately.
   * @param sinkRecords a flat list of the records that need to appear in potentially several files in S3.
   * @param validOffsets an array containing the offsets that map to uploaded files for a topic-partition.
   *                     Offsets appear in ascending order, the difference between two consecutive offsets
   *                     equals the expected size of the file, and last offset is exclusive.
   * @param partitions the set of partitions to verify records for.
   */
  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
                        boolean skipFileListing) throws IOException {
    if (!skipFileListing) {
      verifyFileListing(validOffsets, partitions);
    }

    for (TopicPartition tp : partitions) {
      for (int i = 1, j = 0; i < validOffsets.length; ++i) {
        long startOffset = validOffsets[i - 1];
        long endOffset = validOffsets[i] - 1;

        String filename = FileUtils.committedFileName(url, topicsDir, getDirectory(tp.topic(), tp.partition()), tp,
                                                      startOffset, endOffset, extension, ZERO_PAD_FMT);
        Path path = new Path(filename);
        Collection<Object> records = schemaFileReader.readData(conf, path);

        long size = endOffset - startOffset + 1;
        assertEquals(size, records.size());
        verifyContents(sinkRecords, j, records);
        j += size;
      }
    }
  }

  protected List<String> getExpectedFiles(long[] validOffsets, TopicPartition tp) {
    List<String> expectedFiles = new ArrayList<>();
    for (int i = 1; i < validOffsets.length; ++i) {
      long startOffset = validOffsets[i - 1];
      long endOffset = validOffsets[i] - 1;
      expectedFiles.add(FileUtils.committedFileName(url, topicsDir, getDirectory(tp.topic(), tp.partition()), tp,
                                                    startOffset, endOffset, extension, ZERO_PAD_FMT));
    }
    return expectedFiles;
  }

  protected void verifyFileListing(long[] validOffsets, Set<TopicPartition> partitions) throws IOException {
    for (TopicPartition tp : partitions) {
      verifyFileListing(getExpectedFiles(validOffsets, tp), tp);
    }
  }

  protected void verifyFileListing(List<String> expectedFiles, TopicPartition tp) throws IOException {
    FileStatus[] statuses = {};
    try {
      statuses = fs.listStatus(
          new Path(FileUtils.directoryName(url, topicsDir, getDirectory(tp.topic(), tp.partition()))),
          new TopicPartitionCommittedFileFilter(tp));
    } catch (FileNotFoundException e) {
      // the directory does not exist.
    }

    List<String> actualFiles = new ArrayList<>();
    for (FileStatus status : statuses) {
      actualFiles.add(status.getPath().toString());
    }

    Collections.sort(actualFiles);
    Collections.sort(expectedFiles);
    assertThat(actualFiles, is(expectedFiles));
  }

  protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> records) {
    Schema expectedSchema = null;
    for (Object avroRecord : records) {
      if (expectedSchema == null) {
        expectedSchema = expectedRecords.get(startIndex).valueSchema();
      }
      Object expectedValue = SchemaProjector.project(expectedRecords.get(startIndex).valueSchema(),
                                                     expectedRecords.get(startIndex++).value(),
                                                     expectedSchema);
      assertEquals(avroData.fromConnectData(expectedSchema, expectedValue), avroRecord);
    }
  }

}

