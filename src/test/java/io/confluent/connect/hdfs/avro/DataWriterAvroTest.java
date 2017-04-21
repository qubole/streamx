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

import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.hdfs.storage.StorageFactory;
import io.confluent.connect.hdfs.wal.WAL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataWriterAvroTest extends TestWithMiniDFSCluster {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    schemaFileReader = new AvroFileReader(avroData);
    extension = ".avro";
  }

  @Test
  public void testWriteRecord() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createSinkRecords(7);

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
                                                         endOffset, extension, zeroPadFormat);
      wal.append(tempfile, committedFile);
    }
    wal.append(WAL.endMarker, "");
    wal.close();

    hdfsWriter.recover(TOPIC_PARTITION);
    Map<TopicPartition, Long> offsets = context.offsets();
    assertTrue(offsets.containsKey(TOPIC_PARTITION));
    assertEquals(50L, (long) offsets.get(TOPIC_PARTITION));

    List<SinkRecord> sinkRecords = createSinkRecords(3, 50);

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

    List<SinkRecord> sinkRecords = createSinkRecords(7, 0, assignment);

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
                                                       endOffsets[i], extension, zeroPadFormat));
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

    List<SinkRecord> sinkRecords = createSinkRecords(7, 3);

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

    List<SinkRecord> sinkRecords = createSinkRecords(7, 0, assignment);
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
    sinkRecords = createSinkRecords(3, 6, assignment);

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

    List<SinkRecord> sinkRecords = createSinkRecordsWithAlteringSchemas(7, 0);

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

    List<SinkRecord> sinkRecords = createSinkRecordsWithAlteringSchemas(7, 0);

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
    List<SinkRecord> sinkRecords = createSinkRecordsWithAlteringSchemas(8, 0).subList(1, 8);

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

    List<SinkRecord> sinkRecords = createSinkRecordsNoVersion(1, 0);
    sinkRecords.addAll(createSinkRecordsWithAlteringSchemas(7, 0));

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

    List<SinkRecord> sinkRecords = createSinkRecords(NUMBER_OF_RECORDS);
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

}

