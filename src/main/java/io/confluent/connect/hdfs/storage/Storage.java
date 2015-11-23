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

package io.confluent.connect.hdfs.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;

import io.confluent.connect.hdfs.wal.WAL;

public interface Storage {
  boolean exists(String filename) throws IOException;
  boolean mkdirs(String filename) throws IOException;
  void append(String filename, Object object) throws IOException;
  void delete(String filename) throws IOException;
  void commit(String tempFile, String committedFile) throws IOException;
  void close() throws IOException;
  WAL wal(String topicsDir, TopicPartition topicPart);
  FileStatus[] listStatus(String path, PathFilter filter) throws IOException;
  FileStatus[] listStatus(String path) throws IOException;
  String url();
  Configuration conf();
}
