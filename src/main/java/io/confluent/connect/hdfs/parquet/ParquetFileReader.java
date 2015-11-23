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

package io.confluent.connect.hdfs.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.SchemaFileReader;

public class ParquetFileReader implements SchemaFileReader {

  private AvroData avroData;

  public ParquetFileReader(AvroData avroData) {
    this.avroData = avroData;
  }

  @Override
  public Schema getSchema(Configuration conf, Path path) throws IOException {
    AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>();
    ParquetReader.Builder<GenericRecord> builder = ParquetReader.builder(readSupport, path);
    ParquetReader<GenericRecord> parquetReader = builder.withConf(conf).build();
    GenericRecord record;
    Schema schema = null;
    while ((record = parquetReader.read()) != null) {
      schema = avroData.toConnectSchema(record.getSchema());
    }
    parquetReader.close();
    return schema;
  }

  @Override
  public Collection<Object> readData(Configuration conf, Path path) throws IOException {
    Collection<Object> result = new ArrayList<>();
    AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>();
    ParquetReader.Builder<GenericRecord> builder = ParquetReader.builder(readSupport, path);
    ParquetReader<GenericRecord> parquetReader = builder.withConf(conf).build();
    GenericRecord record;
    while ((record = parquetReader.read()) != null) {
      result.add(record);
    }
    parquetReader.close();
    return result;
  }
}
