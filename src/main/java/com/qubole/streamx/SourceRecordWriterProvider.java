/**
* Copyright 2015 Qubole Inc.
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

package com.qubole.streamx;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriter;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.avro.AvroRecordWriterProvider;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;

public class SourceRecordWriterProvider implements RecordWriterProvider {
  private static final Logger log = LoggerFactory.getLogger(SourceRecordWriterProvider.class);

  @java.lang.Override
  public String getExtension() {
    return "";//No extension
  }

  @java.lang.Override
  public RecordWriter<SinkRecord> getRecordWriter(Configuration conf, final String fileName, SinkRecord record, final AvroData avroData) throws IOException {
    Path path = new Path(fileName);
    final FSDataOutputStream out = path.getFileSystem(conf).create(path);

    return new RecordWriter<SinkRecord>() {
      @Override
      public void write(SinkRecord record) throws IOException {
        log.trace("Sink record: {}", record.toString());
        byte [] values = (byte[])(record.value());
        out.write(values);
        out.write("\n".getBytes());
      }

       @Override
       public void close() throws IOException {
         out.close();
       }
    };
  }
}
