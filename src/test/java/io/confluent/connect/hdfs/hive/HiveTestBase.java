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
 */

package io.confluent.connect.hdfs.hive;

import org.junit.After;
import org.junit.Before;

import java.util.Map;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;

public class HiveTestBase extends TestWithMiniDFSCluster {

  protected String hiveDatabase;
  protected HiveMetaStore hiveMetaStore;
  protected HiveExec hiveExec;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    hiveDatabase = connectorConfig.getString(HdfsSinkConnectorConfig.HIVE_DATABASE_CONFIG);
    hiveMetaStore = new HiveMetaStore(conf, connectorConfig);
    hiveExec = new HiveExec(connectorConfig);
    cleanHive();
  }

  @After
  public void tearDown() throws Exception {
    cleanHive();
    super.tearDown();
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(HdfsSinkConnectorConfig.HIVE_CONF_DIR_CONFIG, "hive_conf");
    return props;
  }

  private void cleanHive() throws Exception {
    // ensures all tables are removed
    for (String database : hiveMetaStore.getAllDatabases()) {
      for (String table : hiveMetaStore.getAllTables(database)) {
        hiveMetaStore.dropTable(database, table);
      }
      if (!"default".equals(database)) {
        hiveMetaStore.dropDatabase(database, false);
      }
    }
  }
}
