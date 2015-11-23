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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.net.URI;

import io.confluent.connect.hdfs.wal.FSWAL;
import io.confluent.connect.hdfs.wal.WAL;

public class HdfsStorage implements Storage {

  private final FileSystem fs;
  private final Configuration conf;
  private final String url;

  public HdfsStorage(Configuration conf,  String url) throws IOException {
    fs = FileSystem.newInstance(URI.create(url), conf);
    this.conf = conf;
    this.url = url;
  }

  @Override
  public FileStatus[] listStatus(String path, PathFilter filter) throws IOException {
    return fs.listStatus(new Path(path), filter);
  }

  @Override
  public FileStatus[] listStatus(String path) throws IOException {
    return fs.listStatus(new Path(path));
  }

  @Override
  public void append(String filename, Object object) throws IOException {

  }

  @Override
  public boolean mkdirs(String filename) throws IOException {
    return fs.mkdirs(new Path(filename));
  }

  @Override
  public boolean exists(String filename) throws IOException {
    return fs.exists(new Path(filename));
  }

  @Override
  public void commit(String tempFile, String committedFile) throws IOException {
    renameFile(tempFile, committedFile);
  }


  @Override
  public void delete(String filename) throws IOException {
    fs.delete(new Path(filename), true);
  }

  @Override
  public void close() throws IOException {
    if (fs != null) {
      fs.close();
    }
  }

  @Override
  public WAL wal(String topicsDir, TopicPartition topicPart) {
    return new FSWAL(topicsDir, topicPart, this);
  }

  @Override
  public Configuration conf() {
    return conf;
  }

  @Override
  public String url() {
    return url;
  }

  private void renameFile(String sourcePath, String targetPath) throws IOException {
    if (sourcePath.equals(targetPath)) {
      return;
    }
    final Path srcPath = new Path(sourcePath);
    final Path dstPath = new Path(targetPath);
    if (fs.exists(srcPath)) {
      fs.rename(srcPath, dstPath);
    }
  }
}
