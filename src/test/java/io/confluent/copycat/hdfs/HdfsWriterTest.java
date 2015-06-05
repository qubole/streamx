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

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import io.confluent.copycat.connector.TopicPartition;
import io.confluent.copycat.data.GenericRecord;
import io.confluent.copycat.errors.CopycatException;
import io.confluent.copycat.sink.SinkRecord;
import io.confluent.copycat.util.AvroData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class HdfsWriterTest extends HdfsSinkConnectorTestBase {

  @Test
  public void testWriteRecord() throws IOException, CopycatException {
    Properties props = createProps();
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);
    HdfsWriter hdfsWriter = new HdfsWriter(connectorConfig);

    String topicsDir = connectorConfig.getString(HdfsSinkConnectorConfig.TOPIC_DIR_CONFIG);
    String topic = "topic";
    int partition = 0;
    String key = "key";
    GenericRecord record = createRecord();
    TopicPartition topicPart = new TopicPartition(topic, partition);

    for (long offset = 0; offset < 7; offset++) {
      SinkRecord sinkRecord = new SinkRecord(topic, partition, key, record, offset);
      hdfsWriter.writeRecord(topicPart, sinkRecord);
    }
    hdfsWriter.close();

    long[] validOffsets = {-1, 2, 5, 6};
    for (int i = 1; i < validOffsets.length; i++) {
      Path path = new Path(FileUtils.committedFileName(url, topicsDir, topicPart, validOffsets[i]));
      Collection<Object> records = readAvroFile(path);
      long size = validOffsets[i] - validOffsets[i - 1];
      assertEquals(records.size(), size);
      for (Object avroRecord: records) {
        assertEquals(avroRecord, AvroData.convertToAvro(record));
      }
    }

    long[] inValidOffsets = {0, 1, 3, 4};
    for (long offset: inValidOffsets) {
      Path path = new Path(FileUtils.committedFileName(url, topicsDir, topicPart, offset));
      assertFalse(fs.exists(path));
    }
  }

  @Test
  public void testGetPreviousOffsets() throws IOException, CopycatException {
    Properties props = createProps();
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    String topicsDir = connectorConfig.getString(HdfsSinkConnectorConfig.TOPIC_DIR_CONFIG);
    String topic = "topic";
    int partition = 0;
    TopicPartition topicPart = new TopicPartition(topic, partition);
    long[] offsets = {3, 6};
    for (long offset: offsets) {
      Path path = new Path(FileUtils.committedFileName(url, topicsDir, topicPart, offset));
      fs.createNewFile(path);
    }
    Path path = new Path(FileUtils.tempFileName(url, topicsDir, topicPart));
    fs.createNewFile(path);

    path = new Path(FileUtils.fileName(url, topicsDir, topicPart, "abcd"));
    fs.createNewFile(path);

    HdfsWriter hdfsWriter = new HdfsWriter(connectorConfig);
    Map<TopicPartition, Long> previousOffsets = hdfsWriter.getPreviousOffsets();
    hdfsWriter.close();

    assertTrue(previousOffsets.containsKey(topicPart));
    long previousOffset = previousOffsets.get(topicPart);
    assertEquals(previousOffset, 6L);
  }

  @Test
  public void testWriteRecordNonZeroInitailOffset() throws IOException, CopycatException {
    Properties props = createProps();
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);
    HdfsWriter hdfsWriter = new HdfsWriter(connectorConfig);

    String topicsDir = connectorConfig.getString(HdfsSinkConnectorConfig.TOPIC_DIR_CONFIG);
    String topic = "topic";
    int partition = 0;
    String key = "key";
    GenericRecord record = createRecord();
    TopicPartition topicPart = new TopicPartition(topic, partition);

    for (long offset = 3; offset < 10; offset++) {
      SinkRecord sinkRecord = new SinkRecord(topic, partition, key, record, offset);
      hdfsWriter.writeRecord(topicPart, sinkRecord);
    }
    hdfsWriter.close();

    long[] validOffsets = {2, 5, 8, 9};
    for (int i = 1; i < validOffsets.length; i++) {
      Path path = new Path(FileUtils.committedFileName(url, topicsDir, topicPart, validOffsets[i]));
      Collection<Object> records = readAvroFile(path);
      long size = validOffsets[i] - validOffsets[i - 1];
      assertEquals(records.size(), size);
      for (Object avroRecord: records) {
        assertEquals(avroRecord, AvroData.convertToAvro(record));
      }
    }

    long[] inValidOffsets = {3, 4, 6, 7};
    for (long offset: inValidOffsets) {
      Path path = new Path(FileUtils.committedFileName(url, topicsDir, topicPart, offset));
      assertFalse(fs.exists(path));
    }
  }
}
