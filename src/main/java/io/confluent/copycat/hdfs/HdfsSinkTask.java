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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import io.confluent.common.config.ConfigException;
import io.confluent.copycat.connector.TopicPartition;
import io.confluent.copycat.errors.CopycatException;
import io.confluent.copycat.errors.CopycatRuntimeException;
import io.confluent.copycat.sink.SinkRecord;
import io.confluent.copycat.sink.SinkTask;

public class HdfsSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(HdfsSinkTask.class);
  private HdfsWriter hdfsWriter;

  public HdfsSinkTask() {

  }

  @Override
  public void start(Properties props) {
    try {
      HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);
      hdfsWriter = new HdfsWriter(connectorConfig);
      context.resetOffset(hdfsWriter.getPreviousOffsets());
    } catch (ConfigException e) {
      throw new CopycatRuntimeException(
          "Couldn't start HdfsSinkConnector due to configuration error.", e);
    }
  }

  @Override
  public void stop() throws CopycatException {
    try {
      hdfsWriter.close();
    } catch (IOException e) {
      throw new CopycatRuntimeException("Error closing the HDFS writer", e);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) throws CopycatException {
    String topic;
    int partition;
    for (SinkRecord record: records) {
      topic = record.getTopic();
      partition = record.getPartition();
      TopicPartition topicPart = new TopicPartition(topic, partition);
      hdfsWriter.writeRecord(topicPart, record);
    }
  }

  @Override
  public void flush(Map<TopicPartition, Long> offsets) {
    // Do nothing as we manage the offset
  }
}
