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

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import io.confluent.copycat.data.Schema;
import io.confluent.copycat.data.GenericRecord;
import io.confluent.copycat.sink.SinkRecord;
import io.confluent.copycat.util.AvroData;

public class AvroRecordWriterProvider implements RecordWriterProvider {

  @Override
  public RecordWriter<Long, SinkRecord> getRecordWriter(Configuration conf, String fileName, SinkRecord record)
      throws IOException{
    DatumWriter<Object> datumWriter = new GenericDatumWriter<Object>();
    final DataFileWriter<Object> writer = new DataFileWriter<Object>(datumWriter);
    Path path = new Path(fileName);
    Schema schema = ((GenericRecord) record.getValue()).getSchema();
    writer.create(AvroData.asAvroSchema(schema), path.getFileSystem(conf).create(path));

    return new RecordWriter<Long, SinkRecord>(){
      @Override
      public void write(Long key, SinkRecord record) throws IOException{
        Object value = AvroData.convertToAvro(record.getValue());
        writer.append(value);
      }

      @Override
      public void close() throws IOException{
        writer.close();
      }
    };
  }
}
