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

package io.confluent.connect.hdfs.avro;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriter;
import io.confluent.connect.hdfs.RecordWriterProvider;

public class AvroRecordWriterProvider implements RecordWriterProvider {

  private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
  private final static String EXTENSION = ".avro";

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter<SinkRecord> getRecordWriter(Configuration conf, final String fileName,
                                                        SinkRecord record, final AvroData avroData)
      throws IOException {
    DatumWriter<Object> datumWriter = new GenericDatumWriter<>();
    final DataFileWriter<Object> writer = new DataFileWriter<>(datumWriter);
    Path path = new Path(fileName);

    final Schema schema = record.valueSchema();
    final FSDataOutputStream out = path.getFileSystem(conf).create(path);
    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
    writer.create(avroSchema, out);

    return new RecordWriter<SinkRecord>(){
      @Override
      public void write(SinkRecord record) throws IOException {
        log.trace("Sink record: {}", record.toString());
        Object value = avroData.fromConnectData(schema, record.value());
        writer.append(value);
      }

      @Override
      public void close() throws IOException {
        writer.close();
      }
    };
  }
}
