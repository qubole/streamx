/**
 * Copyright 2015 Qubole Inc.
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

package com.qubole.streamx.s3;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import io.confluent.connect.hdfs.Version;
import io.confluent.connect.hdfs.HdfsSinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * S3SinkConnector is a Kafka Connect Connector implementation that ingest data from Kafka to S3.
 */
public class S3SinkConnector extends Connector {

  private static final Logger log = LoggerFactory.getLogger(S3SinkConnector.class);
  private Map<String, String> configProperties;
  private static S3SinkConnectorConfig config;

  @Override
  public String version() {
        return Version.getVersion();
    }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    try {
      configProperties = props;
      config = new S3SinkConnectorConfig(props);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start S3SinkConnector due to configuration error", e);
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
        return HdfsSinkTask.class;
    }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    Map<String, String> taskProps = new HashMap<>();
    taskProps.putAll(configProperties);
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() throws ConnectException {
  }

  @Override
  public ConfigDef config() {
        return S3SinkConnectorConfig.getConfig();
    }

  public static String getConfigString(String key) {
    return config.getString(key);
  }
}
