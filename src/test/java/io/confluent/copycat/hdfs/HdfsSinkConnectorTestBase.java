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

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import io.confluent.copycat.data.GenericRecord;
import io.confluent.copycat.data.GenericRecordBuilder;
import io.confluent.copycat.data.Schema;
import io.confluent.copycat.data.SchemaBuilder;

public class HdfsSinkConnectorTestBase {
  protected MiniDFSCluster cluster;
  protected Configuration conf;
  protected String url;
  protected FileSystem fs;

  private MiniDFSCluster createDFSCluster(Configuration conf) throws IOException {
    MiniDFSCluster cluster;
    String[] hosts = {"localhost"};
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    builder.hosts(hosts).nameNodePort(9000);
    cluster = builder.build();
    cluster.waitActive();
    return cluster;
  }

  protected Properties createProps() {
    Properties props = new Properties();
    props.put(HdfsSinkConnectorConfig.HDFS_URL_CONFIG, url);
    props.put(HdfsSinkConnectorConfig.RECORD_WRITER_PROVIDER_CLASS_CONFIG,
              AvroRecordWriterProvider.class.getName());
    props.put(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG, 3);
    return props;
  }

  protected GenericRecord createRecord() {
    Schema schema = SchemaBuilder.record("record").fields()
        .name("null").type().nullType().noDefault()
        .requiredBoolean("boolean")
        .requiredInt("int")
        .requiredLong("long")
        .requiredFloat("float")
        .requiredDouble("double")
        .endRecord();

    return new GenericRecordBuilder(schema)
        .set("null", null)
        .set("boolean", true)
        .set("int", 12)
        .set("long", 12L)
        .set("float", 12.2f)
        .set("double", 12.2)
        .build();
  }

  protected Collection<Object> readAvroFile(Path path) throws IOException {
    Collection<Object> collection = new ArrayList<Object>();
    SeekableInput input = new FsInput(path, conf);
    DatumReader<Object> reader = new GenericDatumReader<Object>();
    FileReader<Object> fileReader = DataFileReader.openReader(input, reader);
    for (Object object: fileReader) {
      collection.add(object);
    }
    fileReader.close();
    return collection;
  }

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    cluster = createDFSCluster(conf);
    url = "hdfs://" + cluster.getNameNode().getClientNamenodeAddress();
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
