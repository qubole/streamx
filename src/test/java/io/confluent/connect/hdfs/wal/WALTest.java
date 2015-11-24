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

package io.confluent.connect.hdfs.wal;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import io.confluent.connect.hdfs.storage.StorageFactory;
import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.hdfs.storage.Storage;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WALTest extends TestWithMiniDFSCluster {
  private static final String ZERO_PAD_FMT = "%010d";

  private boolean closed;
  private static final String extension = ".avro";

  @Test
  @SuppressWarnings("unchecked")
  public void testWALMultiClient() throws Exception {
    fs.delete(new Path(FileUtils.directoryName(url, topicsDir, TOPIC_PARTITION)), true);

    Class<? extends Storage> storageClass = (Class<? extends Storage>)
        Class.forName(connectorConfig.getString(HdfsSinkConnectorConfig.STORAGE_CLASS_CONFIG));
    Storage storage = StorageFactory.createStorage(storageClass, conf, url);

    final WAL wal1 = storage.wal(topicsDir, TOPIC_PARTITION);
    final WAL wal2 = storage.wal(topicsDir, TOPIC_PARTITION);

    String directory = TOPIC + "/" + String.valueOf(PARTITION);
    final String tempfile = FileUtils.tempFileName(url, topicsDir, directory, extension);
    final String commitedFile = FileUtils.committedFileName(url, topicsDir, directory,
                                                            TOPIC_PARTITION, 0, 10, extension,
                                                            ZERO_PAD_FMT);

    fs.createNewFile(new Path(tempfile));

    wal1.acquireLease();
    wal1.append(WAL.beginMarker, "");
    wal1.append(tempfile, commitedFile);
    wal1.append(WAL.endMarker, "");

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // holding the lease for awhile
          Thread.sleep(3000);
          closed = true;
          wal1.close();
        } catch (ConnectException | InterruptedException e) {
          // Ignored
        }
      }
    });
    thread.start();

    wal2.acquireLease();
    assertTrue(closed);
    wal2.apply();
    wal2.close();

    assertTrue(fs.exists(new Path(commitedFile)));
    assertFalse(fs.exists(new Path(tempfile)));
    storage.close();
  }
}
