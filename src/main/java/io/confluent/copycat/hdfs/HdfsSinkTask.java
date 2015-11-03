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

package io.confluent.copycat.hdfs;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.sink.SinkRecord;
import org.apache.kafka.copycat.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import io.confluent.common.config.ConfigException;
import io.confluent.copycat.avro.AvroData;

public class HdfsSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(HdfsSinkTask.class);
  private HdfsWriter hdfsWriter;
  private AvroData avroData;

  public HdfsSinkTask() {

  }

  @Override
  public void start(Properties props) {
    try {
      HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);
      int schemaCacheSize = connectorConfig.getInt(HdfsSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG);
      avroData = new AvroData(schemaCacheSize);
      hdfsWriter = new HdfsWriter(connectorConfig, context, avroData);
      Set<TopicPartition> assignment = context.assignment();
      recover(assignment);
    } catch (ConfigException e) {
      throw new CopycatException(
          "Couldn't start HdfsSinkConnector due to configuration error.", e);
    } catch (CopycatException e) {
      hdfsWriter.close();
    }
  }

  @Override
  public void stop() throws CopycatException {
    hdfsWriter.close();

  }

  @Override
  public void put(Collection<SinkRecord> records) throws CopycatException {
    try {
      hdfsWriter.write(records);
    } catch (IOException e) {
      throw new CopycatException(e);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // Do nothing as the connector manages the offset
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    hdfsWriter.onPartitionsAssigned(partitions);
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    hdfsWriter.onPartitionsRevoked(partitions);
  }

  private void recover(Set<TopicPartition> assignment) {
    for (TopicPartition tp: assignment) {
      hdfsWriter.recover(tp);
    }
  }

  public AvroData getAvroData() {
    return avroData;
  }
}
