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

package io.confluent.connect.hdfs.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;

public class SchemaUtils {

  public static Compatibility getCompatibility(String compatibilityString) {
    switch (compatibilityString) {
      case "BACKWARD":
        return Compatibility.BACKWARD;
      case "FORWARD":
        return Compatibility.FORWARD;
      case "FULL":
        return Compatibility.FULL;
      default:
        return Compatibility.NONE;
    }
  }

  public static boolean shouldChangeSchema(Schema valueSchema, Schema currentSchema, Compatibility compatibility) {
    if (currentSchema == null) {
      return true;
    }
    if ((valueSchema.version() == null || currentSchema.version() == null) && compatibility != Compatibility.NONE) {
      throw new SchemaProjectorException("Schema version required for " + compatibility.toString() + " compatibility");
    }
    switch (compatibility) {
      case BACKWARD:
      case FULL:
        return (valueSchema.version()).compareTo(currentSchema.version()) > 0;
      case FORWARD:
        return (valueSchema.version()).compareTo(currentSchema.version()) < 0;
      default:
        return !valueSchema.equals(currentSchema);
    }
  }


  public static SinkRecord project(SinkRecord record, Schema currentSchema, Compatibility compatibility) {
    switch (compatibility) {
      case BACKWARD:
      case FULL:
      case FORWARD:
        Schema sourceSchema = record.valueSchema();
        Object value = record.value();
        if (sourceSchema == currentSchema || sourceSchema.equals(currentSchema)) {
          return record;
        }
        Object projected = SchemaProjector.project(sourceSchema, value, currentSchema);
        return new SinkRecord(record.topic(), record.kafkaPartition(), record.keySchema(),
                              record.key(), currentSchema, projected, record.kafkaOffset());
      default:
        return record;
    }
  }
}
