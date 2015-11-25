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

import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.avro.AvroFileReader;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.hdfs.storage.StorageFactory;
import io.confluent.connect.hdfs.wal.WAL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HdfsSinkTaskTest extends TestWithMiniDFSCluster {

  private static final String DIRECTORY1 = TOPIC + "/" + "partition=" + String.valueOf(PARTITION);
  private static final String DIRECTORY2 = TOPIC + "/" + "partition=" + String.valueOf(PARTITION2);
  private static final String extension = ".avro";
  private static final String ZERO_PAD_FMT = "%010d";
  private final SchemaFileReader schemaFileReader = new AvroFileReader(avroData);

  @Test
  public void testSinkTaskStart() throws Exception {
    createCommittedFiles();

    Map<String, String> props = createProps();
    HdfsSinkTask task = new HdfsSinkTask();

    task.initialize(context);
    task.start(props);

    Map<TopicPartition, Long> offsets = context.offsets();
    assertEquals(offsets.size(), 2);
    assertTrue(offsets.containsKey(TOPIC_PARTITION));
    assertEquals(21, (long) offsets.get(TOPIC_PARTITION));
    assertTrue(offsets.containsKey(TOPIC_PARTITION2));
    assertEquals(46, (long) offsets.get(TOPIC_PARTITION2));

    task.stop();
  }

  @Test
  public void testSinkTaskStartWithRecovery() throws Exception {
    Map<TopicPartition, List<String>> tempfiles = new HashMap<>();
    List<String> list1 = new ArrayList<>();
    list1.add(FileUtils.tempFileName(url, topicsDir, DIRECTORY1, extension));
    list1.add(FileUtils.tempFileName(url, topicsDir, DIRECTORY1, extension));
    tempfiles.put(TOPIC_PARTITION, list1);

    List<String> list2 = new ArrayList<>();
    list2.add(FileUtils.tempFileName(url, topicsDir, DIRECTORY2, extension));
    list2.add(FileUtils.tempFileName(url, topicsDir, DIRECTORY2, extension));
    tempfiles.put(TOPIC_PARTITION2, list2);

    Map<TopicPartition, List<String>> committedFiles = new HashMap<>();
    List<String> list3 = new ArrayList<>();
    list3.add(FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION, 100, 200,
                                          extension, ZERO_PAD_FMT));
    list3.add(FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION, 201, 300,
                                          extension, ZERO_PAD_FMT));
    committedFiles.put(TOPIC_PARTITION, list3);

    List<String> list4 = new ArrayList<>();
    list4.add(FileUtils.committedFileName(url, topicsDir, DIRECTORY2, TOPIC_PARTITION2, 400, 500,
                                          extension, ZERO_PAD_FMT));
    list4.add(FileUtils.committedFileName(url, topicsDir, DIRECTORY2, TOPIC_PARTITION2, 501, 800,
                                          extension, ZERO_PAD_FMT));
    committedFiles.put(TOPIC_PARTITION2, list4);

    for (TopicPartition tp : tempfiles.keySet()) {
      for (String file : tempfiles.get(tp)) {
        fs.createNewFile(new Path(file));
      }
    }
    createWALs(tempfiles, committedFiles);

    Map<String, String> props = createProps();
    HdfsSinkTask task = new HdfsSinkTask();

    task.initialize(context);
    task.start(props);

    Map<TopicPartition, Long> offsets = context.offsets();
    assertEquals(2, offsets.size());
    assertTrue(offsets.containsKey(TOPIC_PARTITION));
    assertEquals(301, (long) offsets.get(TOPIC_PARTITION));
    assertTrue(offsets.containsKey(TOPIC_PARTITION2));
    assertEquals(801, (long) offsets.get(TOPIC_PARTITION2));

    task.stop();
  }

  @Test
  public void testSinkTaskPut() throws Exception {
    Map<String, String> props = createProps();
    HdfsSinkTask task = new HdfsSinkTask();

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
    task.initialize(context);
    task.start(props);
    task.put(sinkRecords);
    task.stop();

    AvroData avroData = task.getAvroData();
    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {-1, 2, 5};

    for (TopicPartition tp : assignment) {
      String directory = tp.topic() + "/" + "partition=" + String.valueOf(tp.partition());
      for (int j = 1; j < validOffsets.length; ++j) {
        long startOffset = validOffsets[j - 1] + 1;
        long endOffset = validOffsets[j];
        Path path = new Path(FileUtils.committedFileName(url, topicsDir, directory, tp,
                                                         startOffset, endOffset, extension,
                                                         ZERO_PAD_FMT));
        Collection<Object> records = schemaFileReader.readData(conf, path);
        long size = endOffset - startOffset + 1;
        assertEquals(records.size(), size);
        for (Object avroRecord : records) {
          assertEquals(avroRecord, avroData.fromConnectData(schema, record));
        }
      }
    }
  }

  private void createCommittedFiles() throws IOException {
    String file1 = FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION, 0,
                                               10, extension, ZERO_PAD_FMT);
    String file2 = FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION, 11,
                                               20, extension, ZERO_PAD_FMT);
    String file3 = FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION2, 21,
                                               40, extension, ZERO_PAD_FMT);
    String file4 = FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION2, 41,
                                               45, extension, ZERO_PAD_FMT);
    fs.createNewFile(new Path(file1));
    fs.createNewFile(new Path(file2));
    fs.createNewFile(new Path(file3));
    fs.createNewFile(new Path(file4));
  }

  @SuppressWarnings("unchecked")
  private void createWALs(Map<TopicPartition, List<String>> tempfiles,
                          Map<TopicPartition, List<String>> committedFiles) throws Exception {

    Class<? extends Storage> storageClass = (Class<? extends Storage>)
        Class.forName(connectorConfig.getString(HdfsSinkConnectorConfig.STORAGE_CLASS_CONFIG));
    Storage storage = StorageFactory.createStorage(storageClass, conf, url);

    for (TopicPartition tp: tempfiles.keySet()) {
      WAL wal = storage.wal(logsDir, tp);
      List<String> tempList = tempfiles.get(tp);
      List<String> committedList = committedFiles.get(tp);
      wal.append(WAL.beginMarker, "");
      for (int i = 0; i < tempList.size(); ++i) {
        wal.append(tempList.get(i), committedList.get(i));
      }
      wal.append(WAL.endMarker, "");
      wal.close();
    }
  }
}

