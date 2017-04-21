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

package io.confluent.connect.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.hdfs.filter.TopicPartitionCommittedFileFilter;
import io.confluent.connect.hdfs.partitioner.Partitioner;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestWithMiniDFSCluster extends HdfsSinkConnectorTestBase {

  protected MiniDFSCluster cluster;
  protected FileSystem fs;
  protected SchemaFileReader schemaFileReader;
  protected Partitioner partitioner;
  protected String extension;
  // The default based on default configuration of 10
  protected String zeroPadFormat = "%010d";

  @Before
  public void setUp() throws Exception {
    super.setUp();
    conf = new Configuration();
    cluster = createDFSCluster(conf);
    cluster.waitActive();
    url = "hdfs://" + cluster.getNameNode().getClientNamenodeAddress();
    fs = cluster.getFileSystem();
    Map<String, String> props = createProps();
    connectorConfig = new HdfsSinkConnectorConfig(props);
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown(true);
    }
    super.tearDown();
  }

  private MiniDFSCluster createDFSCluster(Configuration conf) throws IOException {
    MiniDFSCluster cluster;
    String[] hosts = {"localhost", "localhost", "localhost"};
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    builder.hosts(hosts).nameNodePort(9001).numDataNodes(3);
    cluster = builder.build();
    cluster.waitActive();
    return cluster;
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(HdfsSinkConnectorConfig.HDFS_URL_CONFIG, url);
    return props;
  }

  /**
   * Return a list of new records starting at zero offset.
   *
   * @param size the number of records to return.
   * @return the list of records.
   */
  protected List<SinkRecord> createSinkRecords(int size) {
    return createSinkRecords(size, 0);
  }

  /**
   * Return a list of new records starting at the given offset.
   *
   * @param size the number of records to return.
   * @param startOffset the starting offset.
   * @return the list of records.
   */
  protected List<SinkRecord> createSinkRecords(int size, long startOffset) {
    return createSinkRecords(size, startOffset, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
  }

  /**
   * Return a list of new records for a set of partitions, starting at the given offset in each partition.
   *
   * @param size the number of records to return.
   * @param startOffset the starting offset.
   * @param partitions the set of partitions to create records for.
   * @return the list of records.
   */
  protected List<SinkRecord> createSinkRecords(int size, long startOffset, Set<TopicPartition> partitions) {
    /*
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
    */

    Schema schema = createSchema();
    Struct record = createRecord(schema);
    List<Struct> same = new ArrayList<>();
    for (int i = 0; i < size; ++i) {
      same.add(record);
    }
    return createSinkRecords(same, schema, startOffset, partitions);
  }

  protected List<SinkRecord> createSinkRecords(List<Struct> records, Schema schema) {
    return createSinkRecords(records, schema, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
  }

  protected List<SinkRecord> createSinkRecords(List<Struct> records, Schema schema, long startOffset,
                                               Set<TopicPartition> partitions) {
    String key = "key";
    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      long offset = startOffset;
      for (Struct record : records) {
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset++));
      }
    }
    return sinkRecords;
  }

  protected List<SinkRecord> createSinkRecordsNoVersion(int size, long startOffset) {
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

  protected List<SinkRecord> createSinkRecordsWithAlteringSchemas(int size, long startOffset) {
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

  protected List<SinkRecord> createSinkRecordsInterleaved(int size, long startOffset, Set<TopicPartition> partitions) {
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
                                                      startOffset, endOffset, extension, zeroPadFormat);
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
                                                    startOffset, endOffset, extension, zeroPadFormat));
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
