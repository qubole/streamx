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

import io.confluent.connect.hdfs.HdfsSinkConnecorConstants;

public class TopicCommittedFileFilter extends CommittedFileFilter {
  private String topic;

  public TopicCommittedFileFilter(String topic) {
    this.topic = topic;
  }

  @Override
  public boolean accept(Path path) {
    if (!super.accept(path)) {
      return false;
    }
    String filename = path.getName();
    String[] parts = filename.split(HdfsSinkConnecorConstants.COMMMITTED_FILENAME_SEPARATOR_REGEX);
    String topic = parts[0];
    return topic.equals(this.topic);
  }
}
