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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import io.confluent.connect.hdfs.utils.Data;
import io.confluent.connect.hdfs.utils.MemoryFormat;
import io.confluent.connect.hdfs.utils.MemoryRecordWriter;
import io.confluent.connect.hdfs.utils.MemoryStorage;

import static org.junit.Assert.assertEquals;

public class FailureRecoveryTest extends HdfsSinkConnectorTestBase {
  private static final String ZERO_PAD_FMT = "%010d";
  private static final String extension = "";

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(HdfsSinkConnectorConfig.STORAGE_CLASS_CONFIG, MemoryStorage.class.getName());
    props.put(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG, MemoryFormat.class.getName());
    return props;
  }

  @Test
  public void testCommitFailure() throws Exception {
    Map<String, String> props = createProps();
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = 0; offset < 7; offset++) {
      SinkRecord sinkRecord =
          new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset);
      sinkRecords.add(sinkRecord);
    }

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    MemoryStorage storage = (MemoryStorage) hdfsWriter.getStorage();
    storage.setFailure(MemoryStorage.Failure.appendFailure);

    hdfsWriter.write(sinkRecords);
    assertEquals(context.timeout(), connectorConfig.getLong(
        HdfsSinkConnectorConfig.RETRY_BACKOFF_CONFIG));

    Map<String, List<Object>> data = Data.getData();

    String logFile = FileUtils.logFileName(url, logsDir, TOPIC_PARTITION);
    List<Object> content = data.get(logFile);
    assertEquals(null, content);

    hdfsWriter.write(new ArrayList<SinkRecord>());
    content = data.get(logFile);
    assertEquals(null, content);

    Thread.sleep(context.timeout());
    hdfsWriter.write(new ArrayList<SinkRecord>());
    content = data.get(logFile);
    assertEquals(6, content.size());

    hdfsWriter.close();
  }

  @Test
  public void testWriterFailureMultiPartitions() throws Exception {
    Map<String, String> props = createProps();
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    ArrayList<SinkRecord> sinkRecords = new ArrayList<>();
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 0L));
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION2, Schema.STRING_SCHEMA, key, schema, record, 0L));

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    hdfsWriter.write(sinkRecords);
    sinkRecords.clear();

    for (long offset = 1; offset < 7; offset++) {
      SinkRecord sinkRecord =
          new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset);
      sinkRecords.add(sinkRecord);
    }

    for (long offset = 1; offset < 7; offset++) {
      SinkRecord sinkRecord =
          new SinkRecord(TOPIC, PARTITION2, Schema.STRING_SCHEMA, key, schema, record, offset);
      sinkRecords.add(sinkRecord);
    }

    String encodedPartition = "partition=" + String.valueOf(PARTITION);
    Map<String, RecordWriter> writers = hdfsWriter.getWriters(TOPIC_PARTITION);
    MemoryRecordWriter writer = (MemoryRecordWriter) writers.get(encodedPartition);
    writer.setFailure(MemoryRecordWriter.Failure.writeFailure);
    hdfsWriter.write(sinkRecords);

    assertEquals(context.timeout(), connectorConfig.getLong(HdfsSinkConnectorConfig.RETRY_BACKOFF_CONFIG));

    Map<String, List<Object>> data = Data.getData();
    String directory2 = TOPIC + "/" + "partition=" + String.valueOf(PARTITION2);
    long[] validOffsets = {-1, 2, 5};
    for (int i = 1; i < validOffsets.length; i++) {
      long startOffset = validOffsets[i - 1] + 1;
      long endOffset = validOffsets[i];
      String path = FileUtils.committedFileName(url, topicsDir, directory2, TOPIC_PARTITION2,
                                                startOffset, endOffset, extension, ZERO_PAD_FMT);
      long size = endOffset - startOffset + 1;
      List<Object> records = data.get(path);
      assertEquals(size, records.size());
    }

    writer.setFailure(MemoryRecordWriter.Failure.closeFailure);
    hdfsWriter.write(new ArrayList<SinkRecord>());
    assertEquals(context.timeout(), connectorConfig.getLong(HdfsSinkConnectorConfig.RETRY_BACKOFF_CONFIG));

    Map<String, String> tempFileNames = hdfsWriter.getTempFileNames(TOPIC_PARTITION);
    String tempFileName = tempFileNames.get(encodedPartition);
    List<Object> content = data.get(tempFileName);
    assertEquals(1, content.size());
    for (int i = 0; i < content.size(); ++i) {
      SinkRecord refSinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, i);
      assertEquals(refSinkRecord, content.get(i));
    }

    Thread.sleep(context.timeout());
    hdfsWriter.write(new ArrayList<SinkRecord>());
    assertEquals(3, content.size());
    for (int i = 0; i < content.size(); ++i) {
      SinkRecord refSinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, i);
      assertEquals(refSinkRecord, content.get(i));
    }

    hdfsWriter.write(new ArrayList<SinkRecord>());
    hdfsWriter.close();
  }

  @Test
  public void testWriterFailure() throws Exception {
    Map<String, String> props = createProps();

    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    ArrayList<SinkRecord> sinkRecords = new ArrayList<>();
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 0L));
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    hdfsWriter.write(sinkRecords);

    sinkRecords.clear();
    for (long offset = 1; offset < 7; offset++) {
      SinkRecord sinkRecord =
          new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset);
      sinkRecords.add(sinkRecord);
    }

    String encodedPartition = "partition=" + String.valueOf(PARTITION);
    Map<String, RecordWriter> writers = hdfsWriter.getWriters(TOPIC_PARTITION);
    MemoryRecordWriter writer = (MemoryRecordWriter) writers.get(encodedPartition);

    writer.setFailure(MemoryRecordWriter.Failure.writeFailure);
    hdfsWriter.write(sinkRecords);
    assertEquals(context.timeout(), connectorConfig.getLong(HdfsSinkConnectorConfig.RETRY_BACKOFF_CONFIG));

    writer.setFailure(MemoryRecordWriter.Failure.closeFailure);
    // nothing happens as we the retry back off hasn't yet passed
    hdfsWriter.write(new ArrayList<SinkRecord>());
    Map<String, List<Object>> data = Data.getData();

    Map<String, String> tempFileNames = hdfsWriter.getTempFileNames(TOPIC_PARTITION);
    String tempFileName = tempFileNames.get(encodedPartition);

    List<Object> content = data.get(tempFileName);
    assertEquals(1, content.size());
    for (int i = 0; i < content.size(); ++i) {
      SinkRecord refSinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, i);
      assertEquals(refSinkRecord, content.get(i));
    }

    Thread.sleep(context.timeout());
    hdfsWriter.write(new ArrayList<SinkRecord>());

    tempFileNames = hdfsWriter.getTempFileNames(TOPIC_PARTITION);
    tempFileName = tempFileNames.get(encodedPartition);

    content = data.get(tempFileName);
    assertEquals(3, content.size());
    for (int i = 0; i < content.size(); ++i) {
      SinkRecord refSinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, i);
      assertEquals(refSinkRecord, content.get(i));
    }

    hdfsWriter.write(new ArrayList<SinkRecord>());
    hdfsWriter.close();
  }
}
