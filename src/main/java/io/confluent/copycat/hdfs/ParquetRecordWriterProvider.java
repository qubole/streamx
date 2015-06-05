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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

import io.confluent.copycat.sink.SinkRecord;
import io.confluent.copycat.util.AvroData;

public class ParquetRecordWriterProvider implements RecordWriterProvider {

  @Override
  public RecordWriter<Long, SinkRecord> getRecordWriter(Configuration conf, String fileName, SinkRecord record)
      throws IOException{
    Object value = AvroData.convertToAvro(record.getValue());
    Schema avroSchema = ((GenericRecord) value).getSchema();

    CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;

    int blockSize = 256 * 1024 * 1024;
    int pageSize = 64 * 1024;

    Path path = new Path(fileName);
    path.getFileSystem(conf).create(path);
    final ParquetWriter<GenericRecord> writer =
        new AvroParquetWriter<GenericRecord>(path, avroSchema, compressionCodecName, blockSize, pageSize);

    return new RecordWriter<Long, SinkRecord>(){
      @Override
      public void write(Long key, SinkRecord record) throws IOException{
        Object value = AvroData.convertToAvro(record.getValue());
        writer.write((GenericRecord) value);
      }

      @Override
      public void close() throws IOException{
        writer.close();
      }
    };
  }
}
