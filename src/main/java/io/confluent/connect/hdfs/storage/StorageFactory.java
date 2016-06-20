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

package io.confluent.connect.hdfs.storage;

import com.qubole.streamx.s3.S3Storage;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.wal.WAL;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.velocity.exception.MethodInvocationException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class StorageFactory {
  public static Storage createStorage(Class<? extends Storage> storageClass, HdfsSinkConnectorConfig config, Configuration conf, String url) {
    try {
      Constructor<? extends Storage> ctor = null;
      if(storageClass == S3Storage.class) {
        ctor = storageClass.getConstructor(Configuration.class, HdfsSinkConnectorConfig.class, String.class);
        return ctor.newInstance(conf, config, url);
      }
      else {
        ctor = storageClass.getConstructor(Configuration.class, String.class);
        return ctor.newInstance(conf, url);
      }
    } catch (NoSuchMethodException | InvocationTargetException | MethodInvocationException | InstantiationException | IllegalAccessException e) {
      throw new ConnectException(e);
    }
  }
}
