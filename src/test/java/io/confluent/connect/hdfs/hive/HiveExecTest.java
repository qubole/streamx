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

package io.confluent.connect.hdfs.hive;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;

import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorTestBase;
import io.confluent.connect.hdfs.HdfsWriter;

import static org.junit.Assert.assertEquals;

public class HiveExecTest extends HdfsSinkConnectorTestBase {

  @Test
  public void testAvroTableImport() throws Exception {
    Schema avroSchema = avroData.fromConnectSchema(createSchema());
    HiveExec hiveExec = new HiveExec(conf, connectorConfig);
    runHive(hiveExec, "DROP TABLE " + TOPIC);
    hiveExec.createAvroTable(TOPIC, avroSchema);

    String tablesInHive = runHive(hiveExec, "SHOW TABLES");
    assertEquals(tablesInHive, TOPIC);

    String description = runHive(hiveExec, "DESCRIBE " + TOPIC);
    String[] colTypes = {"boolean", "int", "bigint", "float", "double"};
    String[] lines = description.split("\n");
    for (int i = 0; i < colTypes.length; ++i) {
      String[] parts = lines[i].replace(" ", "").split("\t");
      assertEquals(colTypes[i], parts[1]);
    }
  }

  @Test
  public void testAddPartition() throws Exception {
    prepareData(TOPIC, PARTITION);

    Schema avroSchema = avroData.fromConnectSchema(createSchema());
    HiveExec hiveExec = new HiveExec(conf, connectorConfig);

    runHive(hiveExec, "DROP TABLE " + TOPIC);

    hiveExec.createAvroTable(TOPIC, avroSchema);
    hiveExec.addPartition(TOPIC, PARTITION);
    hiveExec.setPartitionLoc(TOPIC, PARTITION);

    String[] expectedResult = {"true", "12", "12", "12.2", "12.2", "12"};
    String result = runHive(hiveExec, "SELECT * from " + TOPIC);
    String[] rows = result.split("\n");
    assertEquals(7, rows.length);
    for (String row: rows) {
      String[] parts  = parseOutput(row);
      for (int j = 0; j < expectedResult.length; ++j) {
        assertEquals(expectedResult[j], parts[j]);
      }
    }
  }

  @Test
  public void testAlterSchema() throws Exception {
    prepareData(TOPIC, PARTITION);

    Schema avroSchema = avroData.fromConnectSchema(createSchema());
    HiveExec hiveExec = new HiveExec(conf, connectorConfig);

    runHive(hiveExec, "DROP TABLE " + TOPIC);
    hiveExec.createAvroTable(TOPIC, avroSchema);
    hiveExec.addPartition(TOPIC, PARTITION);
    hiveExec.setPartitionLoc(TOPIC, PARTITION);

    Schema newAvroSchema = avroData.fromConnectSchema(createNewSchema());

    hiveExec.alterSchema(TOPIC, newAvroSchema);
    String description = runHive(hiveExec, "DESCRIBE " + TOPIC);
    String[] colTypes = {"boolean", "int", "bigint", "float", "double", "string"};
    String[] lines = description.split("\n");

    for (int i = 0; i < colTypes.length; ++i) {
      String[] parts = parseOutput(lines[i]);
      assertEquals(colTypes[i], parts[1]);
    }

    String[] expectedResult = {"true", "12", "12", "12.2", "12.2", "abc", "12"};
    String result = runHive(hiveExec, "SELECT * from " + TOPIC);
    String[] rows = result.split("\n");
    assertEquals(7, rows.length);
    for (String row: rows) {
      String[] parts  = parseOutput(row);
      for (int j = 0; j < expectedResult.length; ++j) {
        assertEquals(expectedResult[j], parts[j]);
      }
    }
  }

  private void prepareData(String topic, int partition) throws Exception {
    TopicPartition tp = new TopicPartition(topic, partition);
    HdfsWriter hdfsWriter = new HdfsWriter(connectorConfig, context, avroData);
    hdfsWriter.recover(tp);

    String key = "key";
    org.apache.kafka.connect.data.Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = 0; offset < 7; offset++) {
      SinkRecord sinkRecord =
          new SinkRecord(topic, partition, org.apache.kafka.connect.data.Schema.STRING_SCHEMA, key,
                         schema, record, offset);
      sinkRecords.add(sinkRecord);
    }
    hdfsWriter.write(sinkRecords);

    hdfsWriter.close();
    // TODO: Move WALs to proper directories
    fs.delete(new Path(FileUtils.logFileName(url, topicsDir, tp)), false);
  }

  private String[] parseOutput(String output) {
    return output.replace(" ", "").split("\t");
  }

  private String runHive(HiveExec hiveExec, String query) throws Exception {
    ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
    PrintStream outSaved = System.out;
    PrintStream errSaved = System.err;
    System.setOut(new PrintStream(outBytes, true));
    System.setErr(new PrintStream(errBytes, true));
    try {
      hiveExec.executeQuery(query);
    } finally {
      System.setOut(outSaved);
      System.setErr(errSaved);
    }
    ByteArrayInputStream outBytesIn = new ByteArrayInputStream(outBytes.toByteArray());
    ByteArrayInputStream errBytesIn = new ByteArrayInputStream(errBytes.toByteArray());
    BufferedReader is = new BufferedReader(new InputStreamReader(outBytesIn));
    BufferedReader es = new BufferedReader(new InputStreamReader(errBytesIn));
    StringBuilder output = new StringBuilder();
    String line;
    while ((line = is.readLine()) != null) {
      if (output.length() > 0) {
        output.append("\n");
      }
      output.append(line);
    }
    if (output.length() == 0) {
      output = new StringBuilder();
      while ((line = es.readLine()) != null) {
        output.append("\n");
        output.append(line);
      }
    }
    return output.toString();
  }
}
