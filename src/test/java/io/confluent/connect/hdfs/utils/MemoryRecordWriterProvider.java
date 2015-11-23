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

package io.confluent.connect.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriter;
import io.confluent.connect.hdfs.RecordWriterProvider;

public class MemoryRecordWriterProvider implements RecordWriterProvider {

  @Override
  public String getExtension() {
    return "";
  }

  @Override
  public RecordWriter<SinkRecord> getRecordWriter(
      Configuration conf, final String fileName, SinkRecord record, final AvroData avroData)
      throws IOException {

    final Map<String, List<Object>> data = Data.getData();

    if (!data.containsKey(fileName)) {
      data.put(fileName, new LinkedList<>());
    }

    return new MemoryRecordWriter(fileName);
  }


}
