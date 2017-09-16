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
 */

package io.confluent.connect.hdfs.avro;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.hive.HiveTestBase;
import io.confluent.connect.hdfs.hive.HiveTestUtils;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.partitioner.Partitioner;

import static org.junit.Assert.assertEquals;

public class AvroHiveUtilTest extends HiveTestBase {

  private HiveUtil hive;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    hive = new AvroHiveUtil(connectorConfig, avroData, hiveMetaStore);
  }

  @Test
  public void testCreateTable() throws Exception {
    prepareData(TOPIC, PARTITION);
    Partitioner partitioner = HiveTestUtils.getPartitioner();

    Schema schema = createSchema();
    hive.createTable(hiveDatabase, TOPIC, schema, partitioner);
    String location = "partition=" + String.valueOf(PARTITION);
    hiveMetaStore.addPartition(hiveDatabase, TOPIC, location);

    Struct expectedRecord = createRecord(schema);
    List<String> expectedResult = new ArrayList<>();
    List<String> expectedColumnNames = new ArrayList<>();
    for (Field field : schema.fields()) {
      expectedColumnNames.add(field.name());
      expectedResult.add(String.valueOf(expectedRecord.get(field.name())));
    }

    Table table = hiveMetaStore.getTable(hiveDatabase, TOPIC);
    List<String> actualColumnNames = new ArrayList<>();
    for (FieldSchema column : table.getSd().getCols()) {
      actualColumnNames.add(column.getName());
    }

    assertEquals(expectedColumnNames, actualColumnNames);
    List<FieldSchema> partitionCols = table.getPartitionKeys();
    assertEquals(1, partitionCols.size());
    assertEquals("partition", partitionCols.get(0).getName());

    String result = HiveTestUtils.runHive(hiveExec, "SELECT * FROM " + TOPIC);
    String[] rows = result.split("\n");
    // Only 6 of the 7 records should have been delivered due to flush_size = 3
    assertEquals(6, rows.length);
    for (String row : rows) {
      String[] parts = HiveTestUtils.parseOutput(row);
      int j = 0;
      for (String expectedValue : expectedResult) {
        assertEquals(expectedValue, parts[j++]);
      }
    }
  }

  @Test
  public void testAlterSchema() throws Exception {
    prepareData(TOPIC, PARTITION);
    Partitioner partitioner = HiveTestUtils.getPartitioner();
    Schema schema = createSchema();
    hive.createTable(hiveDatabase, TOPIC, schema, partitioner);

    String location = "partition=" + String.valueOf(PARTITION);
    hiveMetaStore.addPartition(hiveDatabase, TOPIC, location);

    Struct expectedRecord = createRecord(schema);
    List<String> expectedResult = new ArrayList<>();
    List<String> expectedColumnNames = new ArrayList<>();
    for (Field field : schema.fields()) {
      expectedColumnNames.add(field.name());
      expectedResult.add(String.valueOf(expectedRecord.get(field.name())));
    }

    Table table = hiveMetaStore.getTable(hiveDatabase, TOPIC);
    List<String> actualColumnNames = new ArrayList<>();
    for (FieldSchema column : table.getSd().getCols()) {
      actualColumnNames.add(column.getName());
    }

    assertEquals(expectedColumnNames, actualColumnNames);

    Schema newSchema = createNewSchema();

    hive.alterSchema(hiveDatabase, TOPIC, newSchema);

    String result = HiveTestUtils.runHive(hiveExec, "SELECT * from " + TOPIC);
    String[] rows = result.split("\n");
    // Only 6 of the 7 records should have been delivered due to flush_size = 3
    assertEquals(6, rows.length);
    for (String row : rows) {
      String[] parts = HiveTestUtils.parseOutput(row);
      int j = 0;
      for (String expectedValue : expectedResult) {
        assertEquals(expectedValue, parts[j++]);
      }
    }
  }

  private void prepareData(String topic, int partition) throws Exception {
    TopicPartition tp = new TopicPartition(topic, partition);
    DataWriter hdfsWriter = createWriter(context, avroData);
    hdfsWriter.recover(tp);

    List<SinkRecord> sinkRecords = createSinkRecords(7);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close(assignment);
    hdfsWriter.stop();
  }

  private DataWriter createWriter(SinkTaskContext context, AvroData avroData){
    return new DataWriter(connectorConfig, context, avroData);
  }
}
