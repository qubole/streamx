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

package io.confluent.connect.hdfs.hive;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HiveSchemaConverterTest {

  private static final Schema SIMPLE_STRUCT = SchemaBuilder.struct().name("SimpleStruct")
      .field("id", Schema.INT32_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .build();

  private static final Schema COMPLEX_STRUCT = SchemaBuilder.struct().name("ComplexStruct")
      .field("groupName", Schema.STRING_SCHEMA)
      .field("simpleStructs", SchemaBuilder.array(SIMPLE_STRUCT).build())
      .build();


  @Test
  public void testConvertSimpleStruct() {
    TypeInfo type = HiveSchemaConverter.convert(SIMPLE_STRUCT);
    assertTrue(type instanceof StructTypeInfo);

    List<String> expectedFieldNames = new ArrayList<>();
    expectedFieldNames.add("id");
    expectedFieldNames.add("name");

    assertEquals(expectedFieldNames, ((StructTypeInfo) type).getAllStructFieldNames());
  }

  @Test
  public void testConvertComplexStruct() {
    List<FieldSchema> fields = HiveSchemaConverter.convertSchema(COMPLEX_STRUCT);
    List<String> expectedFieldNames = new ArrayList<>();
    expectedFieldNames.add("groupName");
    expectedFieldNames.add("simpleStructs");

    List<String> actualFieldNames = new ArrayList<>();
    for (FieldSchema fieldSchema: fields) {
      actualFieldNames.add(fieldSchema.getName());
    }
    assertEquals(expectedFieldNames, actualFieldNames);

    List<String> expectedTypes = new ArrayList<>();
    List<TypeInfo> typeInfos = new ArrayList<>();
    typeInfos.add(TypeInfoFactory.intTypeInfo);
    typeInfos.add(TypeInfoFactory.stringTypeInfo);

    expectedTypes.add(TypeInfoFactory.stringTypeInfo.toString());

    List<String> expectedInnerFieldNames = new ArrayList<>();
    expectedInnerFieldNames.add("id");
    expectedInnerFieldNames.add("name");
    TypeInfo structType = TypeInfoFactory.getStructTypeInfo(expectedInnerFieldNames, typeInfos);

    expectedTypes.add(TypeInfoFactory.getListTypeInfo(structType).toString());

    List<String> actualTypes = new ArrayList<>();
    for (FieldSchema fieldSchema: fields) {
      actualTypes.add(fieldSchema.getType());
    }
    assertEquals(expectedTypes, actualTypes);
  }

  @Test
  public void testConvertArray() {
    TypeInfo type = HiveSchemaConverter.convert(SchemaBuilder.array(Schema.FLOAT32_SCHEMA));
    assertEquals(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.floatTypeInfo), type);
  }

  @Test
  public void testConvertMap() {
    TypeInfo type = HiveSchemaConverter.convert(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA));
    assertEquals(TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.doubleTypeInfo), type);

  }
}
