/**
 * Copyright 2016 Confluent Inc.
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

package io.confluent.connect.hdfs.wal;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.hdfs.storage.Storage;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FSWALTest extends TestWithMiniDFSCluster {
  @Test
  public void testTruncate() throws Exception {
    Storage storage = new HdfsStorage(conf, url);
    TopicPartition tp = new TopicPartition("mytopic", 123);
    FSWAL wal = new FSWAL("/logs", tp, storage);
    wal.append("a", "b");
    assertTrue("WAL file should exist after append",
            storage.exists("/logs/mytopic/123/log"));
    wal.truncate();
    assertFalse("WAL file should not exist after truncate",
            storage.exists("/logs/mytopic/123/log"));
    assertTrue("Rotated WAL file should exist after truncate",
            storage.exists("/logs/mytopic/123/log.1"));
    wal.append("c", "d");
    assertTrue("WAL file should be recreated after truncate + append",
            storage.exists("/logs/mytopic/123/log"));
    assertTrue("Rotated WAL file should exist after truncate + append",
            storage.exists("/logs/mytopic/123/log.1"));
  }
}
