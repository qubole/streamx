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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.RecordWriter;

public class ParquetRecordWriterProvider implements RecordWriterProvider {

  private final static String EXTENSION = ".parquet";

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter<SinkRecord> getRecordWriter(
      Configuration conf, final String fileName, SinkRecord record, final AvroData avroData)
      throws IOException {
    final Schema avroSchema = avroData.fromConnectSchema(record.valueSchema());
    CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;

    int blockSize = 256 * 1024 * 1024;
    int pageSize = 64 * 1024;

    Path path = new Path(fileName);
    final ParquetWriter<GenericRecord> writer =
        new AvroParquetWriter<>(path, avroSchema, compressionCodecName, blockSize, pageSize);

    return new RecordWriter<SinkRecord>() {
      @Override
      public void write(SinkRecord record) throws IOException {
        Object value = avroData.fromConnectData(record.valueSchema(), record.value());
        writer.write((GenericRecord) value);
      }

      @Override
      public void close() throws IOException {
        writer.close();
      }
    };
  }
}
