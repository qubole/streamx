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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.errors.PartitionException;

public class FieldPartitioner implements Partitioner {
  private static final Logger log = LoggerFactory.getLogger(FieldPartitioner.class);
  private List<String> fieldNames;
  private List<FieldSchema> partitionFields = new ArrayList<>();

  @Override
  public void configure(Map<String, Object> config) {
    fieldNames = (List<String>) config.get(HdfsSinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG);
    for (String fieldName : fieldNames) {
      partitionFields.add(new FieldSchema(fieldName, TypeInfoFactory.stringTypeInfo.toString(), ""));
    }
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    Object value = sinkRecord.value();
    Schema valueSchema = sinkRecord.valueSchema();
    StringBuilder partition = new StringBuilder();
    if (value instanceof Struct) {
      Struct struct = (Struct) value;

      for (String fieldName : fieldNames) {
        partition.append("/").append(fieldName).append("=");

        Object partitionKey = struct.get(fieldName);
        Type type = valueSchema.field(fieldName).schema().type();
        switch (type) {
          case INT8:
          case INT16:
          case INT32:
          case INT64:
            Number record = (Number) partitionKey;
            partition.append(record);
            break;
          case STRING:
            partition.append(partitionKey);
            break;
          case BOOLEAN:
            boolean booleanRecord = (boolean) partitionKey;
            partition.append(booleanRecord);
            break;
          default:
            log.error("Type {} is not supported as a partition key.", type.getName());
            throw new PartitionException("Error encoding partition.");
        }
      }
    } else {
      log.error("Value is not Struct type.");
      throw new PartitionException("Error encoding partition.");
    }

    return partition.toString();
  }

  @Override
  public String generatePartitionedPath(String topic, String encodedPartition) {
    return topic + encodedPartition;
  }

  @Override
  public List<FieldSchema> partitionFields() {
    return partitionFields;
  }
}
