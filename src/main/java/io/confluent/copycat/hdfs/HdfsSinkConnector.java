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

package io.confluent.copycat.hdfs;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import io.confluent.common.config.ConfigException;
import io.confluent.copycat.connector.Connector;
import io.confluent.copycat.connector.Task;
import io.confluent.copycat.errors.CopycatException;
import io.confluent.copycat.errors.CopycatRuntimeException;

/**
 * HdfsSinkConnector is a Copycat Connector implementation that ingest data
 * from Kafka to HDFS.
 */
public class HdfsSinkConnector extends Connector{

  private Properties configProperties;
  private HdfsSinkConnectorConfig config;

  @Override
  public void start(Properties props) throws CopycatException {
    try {
      configProperties = props;
      config = new HdfsSinkConnectorConfig(props);
    } catch (ConfigException e) {
      throw new CopycatRuntimeException("Couldn't start HdfsSinkConnector due to configuration "
                                         + "error", e);
    }
  }

  @Override
  public Class<? extends Task> getTaskClass() {
    return HdfsSinkTask.class;
  }

  @Override
  public List<Properties> getTaskConfigs(int maxTasks) {
    List<Properties> taskConfigs = new ArrayList<Properties>();
    Properties taskProps = new Properties();
    taskProps.putAll(configProperties);
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() throws CopycatException {

  }
}
