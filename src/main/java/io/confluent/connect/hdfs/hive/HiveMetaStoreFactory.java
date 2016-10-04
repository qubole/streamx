package io.confluent.connect.hdfs.hive;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RpcNoSuchMethodException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.velocity.exception.MethodInvocationException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class HiveMetaStoreFactory {
  public static HiveMetaStore createHiveMetaStore(Class<? extends HiveMetaStore> metaStoreClass, Configuration conf, HdfsSinkConnectorConfig connectorConfig) {
    try {
      Constructor<? extends HiveMetaStore> ctor =
          metaStoreClass.getConstructor(Configuration.class, HdfsSinkConnectorConfig.class);
      return ctor.newInstance(conf, connectorConfig);
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new ConnectException(e);
    }
  }
}
