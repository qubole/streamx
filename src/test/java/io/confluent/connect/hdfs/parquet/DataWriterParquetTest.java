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

package io.confluent.connect.hdfs.parquet;


import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.SchemaFileReader;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.hdfs.partitioner.Partitioner;

import static org.junit.Assert.assertEquals;

public class DataWriterParquetTest extends TestWithMiniDFSCluster {
  private static final String ZERO_PAD_FMT = "%010d";
  private static final String extension = ".parquet";
  private final SchemaFileReader schemaFileReader = new ParquetFileReader(avroData);

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG, ParquetFormat.class.getName());
    return props;
  }

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
      Path path = new Path(
          FileUtils.committedFileName(url, topicsDir, directory, TOPIC_PARTITION, startOffset,
                                      endOffset, extension, ZERO_PAD_FMT));
      Collection<Object> records = schemaFileReader.readData(conf, path);
      long size = endOffset - startOffset + 1;
      assertEquals(size, records.size());
      for (Object avroRecord : records) {
        assertEquals(avroData.fromConnectData(schema, record), avroRecord);
      }
    }
  }
}
