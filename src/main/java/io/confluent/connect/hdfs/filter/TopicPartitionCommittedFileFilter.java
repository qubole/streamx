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

package io.confluent.connect.hdfs.filter;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;

import io.confluent.connect.hdfs.HdfsSinkConnector;
import io.confluent.connect.hdfs.HdfsSinkConnectorConstants;

import java.util.regex.Matcher;

public class TopicPartitionCommittedFileFilter extends CommittedFileFilter {
  private TopicPartition tp;

  public TopicPartitionCommittedFileFilter(TopicPartition tp) {
    this.tp = tp;
  }

  @Override
  public boolean accept(Path path) {
    if (!super.accept(path)) {
      return false;
    }
    String filename = path.getName();
    Matcher m = HdfsSinkConnectorConstants.COMMITTED_FILENAME_PATTERN.matcher(filename);
    // NB: if statement has side effect of enabling group() call
    if (!m.matches()) {
      throw new AssertionError("match expected because of CommittedFileFilter");
    }
    String topic = m.group(HdfsSinkConnectorConstants.PATTERN_TOPIC_GROUP);
    int partition = Integer.parseInt(m.group(HdfsSinkConnectorConstants.PATTERN_PARTITION_GROUP));
    return topic.equals(tp.topic()) && partition == tp.partition();
  }
}
