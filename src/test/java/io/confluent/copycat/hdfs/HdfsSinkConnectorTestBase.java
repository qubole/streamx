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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.data.SchemaBuilder;
import org.apache.kafka.copycat.data.Struct;
import org.apache.kafka.copycat.sink.SinkTaskContext;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import io.confluent.copycat.avro.AvroData;

public class HdfsSinkConnectorTestBase {
  protected MiniDFSCluster cluster;
  protected Configuration conf;
  protected String url;
  protected FileSystem fs;
  protected HdfsSinkConnectorConfig connectorConfig;
  protected String topicsDir;
  protected AvroData avroData;
  protected boolean dfsCluster = true;

  protected MockSinkTaskContext context;
  protected static final String TOPIC = "topic";
  protected static final int PARTITION = 12;
  protected static final int PARTITION2 = 13;
  protected static final int PARTITION3 = 14;
  protected static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);
  protected static final TopicPartition TOPIC_PARTITION2 = new TopicPartition(TOPIC, PARTITION2);
  protected static final TopicPartition TOPIC_PARTITION3 = new TopicPartition(TOPIC, PARTITION3);
  protected static Set<TopicPartition> assignment;

  private MiniDFSCluster createDFSCluster(Configuration conf) throws IOException {
    MiniDFSCluster cluster;
    String[] hosts = {"localhost", "localhost", "localhost"};
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    builder.hosts(hosts).nameNodePort(9000).numDataNodes(3);
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
    props.put(HdfsSinkConnectorConfig.ROTATE_INTERVAL_CONFIG, 10);
    return props;
  }

  protected Schema createSchema() {
    return SchemaBuilder.struct().name("record")
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("int", Schema.INT32_SCHEMA)
        .field("long", Schema.INT64_SCHEMA)
        .field("float", Schema.FLOAT32_SCHEMA)
        .field("double", Schema.FLOAT64_SCHEMA)
        .build();
  }

  protected Struct createRecord(Schema schema) {
    return new Struct(schema)
        .put("boolean", true)
        .put("int", 12)
        .put("long", 12L)
        .put("float", 12.2f)
        .put("double", 12.2);
  }

  protected Collection<Object> readAvroFile(Path path) throws IOException {
    Collection<Object> collection = new ArrayList<>();
    SeekableInput input = new FsInput(path, conf);
    DatumReader<Object> reader = new GenericDatumReader<>();
    FileReader<Object> fileReader = DataFileReader.openReader(input, reader);
    for (Object object: fileReader) {
      collection.add(object);
    }
    fileReader.close();
    return collection;
  }

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    if (dfsCluster) {
      cluster = createDFSCluster(conf);
      cluster.waitActive();
      url = "hdfs://" + cluster.getNameNode().getClientNamenodeAddress();
      fs = cluster.getFileSystem();
    } else {
      url = "memory://";
    }
    Properties props = createProps();
    connectorConfig = new HdfsSinkConnectorConfig(props);
    topicsDir = connectorConfig.getString(HdfsSinkConnectorConfig.TOPIC_DIR_CONFIG);
    int schemaCacheSize = connectorConfig.getInt(HdfsSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG);
    avroData = new AvroData(schemaCacheSize);
    assignment = new HashSet<>();
    assignment.add(TOPIC_PARTITION);
    assignment.add(TOPIC_PARTITION2);
    context = new MockSinkTaskContext();
  }

  @After
  public void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown(true);
    }
    if (assignment != null) {
      assignment.clear();
    }
  }

  protected static class MockSinkTaskContext extends SinkTaskContext {

    public Map<TopicPartition, Long> offsets() {
      return offsets;
    }

    public long backoff() {
      return timeoutMs;
    }

    @Override
    public Set<TopicPartition> assignment() {
      return assignment;
    }

    @Override
    public void pause(TopicPartition... partitions) {
      return;
    }

    @Override
    public void resume(TopicPartition... partitions) {
      return;
    }
  }
}
