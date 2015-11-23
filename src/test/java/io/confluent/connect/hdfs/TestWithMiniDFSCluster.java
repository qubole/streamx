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

package io.confluent.connect.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

public class TestWithMiniDFSCluster extends HdfsSinkConnectorTestBase {

  protected MiniDFSCluster cluster;
  protected FileSystem fs;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    conf = new Configuration();
    cluster = createDFSCluster(conf);
    cluster.waitActive();
    url = "hdfs://" + cluster.getNameNode().getClientNamenodeAddress();
    fs = cluster.getFileSystem();
    Map<String, String> props = createProps();
    connectorConfig = new HdfsSinkConnectorConfig(props);
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown(true);
    }
    super.tearDown();
  }

  private MiniDFSCluster createDFSCluster(Configuration conf) throws IOException {
    MiniDFSCluster cluster;
    String[] hosts = {"localhost", "localhost", "localhost"};
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    builder.hosts(hosts).nameNodePort(9001).numDataNodes(3);
    cluster = builder.build();
    cluster.waitActive();
    return cluster;
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(HdfsSinkConnectorConfig.HDFS_URL_CONFIG, url);
    return props;
  }
}
