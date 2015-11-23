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

package io.confluent.connect.hdfs.partitioner;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DefaultPartitioner implements Partitioner {

  private static final String partitionField = "partition";
  private final List<FieldSchema> partitionFields =  new ArrayList<>();;

  @Override
  public void configure(Map<String, Object> config) {
    partitionFields.add(new FieldSchema(partitionField, TypeInfoFactory.stringTypeInfo.toString(), ""));
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    return partitionField + "=" + String.valueOf(sinkRecord.kafkaPartition());
  }

  @Override
  public String generatePartitionedPath(String topic, String encodedPartition) {
    return topic + "/" + encodedPartition;
  }

  @Override
  public List<FieldSchema> partitionFields() {
    return partitionFields;
  }
}
