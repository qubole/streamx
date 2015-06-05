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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import io.confluent.copycat.connector.TopicPartition;
import io.confluent.copycat.errors.CopycatException;
import io.confluent.copycat.errors.CopycatRuntimeException;
import io.confluent.copycat.sink.SinkRecord;


public class HdfsWriter {
  private static final Logger log = LoggerFactory.getLogger(HdfsWriter.class);
  private Map<TopicPartition, RecordWriter<Long, SinkRecord>> writers = null;
  private Map<TopicPartition, Long> offsets = null;
  private Map<TopicPartition, Integer> recordCounters = null;
  private Configuration conf;
  private FileSystem fs;
  private Class<? extends RecordWriterProvider> writerProviderClass;
  private String url;
  private int flushSize;
  private String topicsDir;

  @SuppressWarnings("unchecked")
  public HdfsWriter(HdfsSinkConnectorConfig connectorConfig) {
    try {
      writers = new HashMap<TopicPartition, RecordWriter<Long, SinkRecord>>();
      recordCounters = new HashMap<TopicPartition, Integer>();
      flushSize = connectorConfig.getInt(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG);

      url = connectorConfig.getString(HdfsSinkConnectorConfig.HDFS_URL_CONFIG);
      topicsDir = connectorConfig.getString(HdfsSinkConnectorConfig.TOPIC_DIR_CONFIG);
      conf = new Configuration();
      fs = FileSystem.newInstance(URI.create(url), conf);
      Path path = new Path(url + "/" + topicsDir);
      if (!fs.exists(path)) {
        fs.mkdirs(path);
      }
      offsets = readOffsets();
      writerProviderClass = (Class<RecordWriterProvider>)
          Class.forName(connectorConfig.getString(HdfsSinkConnectorConfig.RECORD_WRITER_PROVIDER_CLASS_CONFIG));
    } catch (IOException e) {
      throw new CopycatRuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new CopycatRuntimeException("RecordWriterProvider class not found", e);
    }
  }


  public Map<TopicPartition, Long> getPreviousOffsets() {
    return offsets;
  }

  public void writeRecord(TopicPartition topicPart, SinkRecord record)
      throws CopycatException{
    try {
      RecordWriter<Long, SinkRecord> writer = getWriter(topicPart, record);
      writer.write(System.currentTimeMillis(), record);
      if (!offsets.containsKey(topicPart)) {
        offsets.put(topicPart, record.getOffset() - 1);
      }
      updateRecordCounter(topicPart);
      if (shouldRotate(topicPart)) {
        rotate(topicPart);
      }
    } catch (IOException e) {
      throw new CopycatRuntimeException(e);
    }
  }

  public void close() throws IOException, CopycatException {
    for (TopicPartition topicPart: writers.keySet()) {
      rotate(topicPart);
    }
    writers.clear();
    if (fs != null) {
      fs.close();
    }
  }

  private Map<TopicPartition, Long> readOffsets() throws IOException {
    Map<TopicPartition, Long> offsets = new HashMap<TopicPartition, Long>();
    Path path = new Path(url + "/" + topicsDir);
    PathFilter filter = new CommittedFileFilter();
    FileStatus[] topics = fs.listStatus(path);
    for (FileStatus topic: topics) {
      if (topic.isDirectory()) {
        String topicName = topic.getPath().getName();
        FileStatus[] partitions = fs.listStatus(topic.getPath(), filter);
        for (FileStatus partition : partitions) {
          if (partition.isFile()) {
            String filename = partition.getPath().getName();
            String[] parts = filename.split("\\.");
            try {
              int partitionNum = Integer.parseInt(parts[0]);
              long offset = Long.parseLong(parts[1]);
              TopicPartition topicPart = new TopicPartition(topicName, partitionNum);
              if (!offsets.containsKey(topicPart) || offset > offsets.get(topicPart)) {
                offsets.put(topicPart, offset);
              }
            } catch (NumberFormatException e) {
              log.warn("Invalid Copycat HDFS file: {}", filename);
            }
          }
        }
      }
    }
    return offsets;
  }

  private class CommittedFileFilter implements PathFilter {
    public boolean accept(Path path) {
      String[] parts = path.getName().split("\\.");
      return !(parts.length != 2 || parts[1].equals("tmp"));
    }
  }

  private RecordWriter<Long, SinkRecord> getWriter(TopicPartition topicPart, SinkRecord record)
      throws CopycatException {
    try {
      if (writers.containsKey(topicPart)) return writers.get(topicPart);
      String fileName = FileUtils.tempFileName(url, topicsDir, topicPart);
      Path path = new Path(fileName);
      if (fs.exists(path)) {
        log.info("Deleting uncommited file {}", fileName);
        fs.delete(path, false);
      }
      RecordWriter<Long, SinkRecord> writer = writerProviderClass.newInstance().
          getRecordWriter(conf, fileName, record);
      writers.put(topicPart, writer);
      recordCounters.put(topicPart, 0);
      return writer;
    } catch (IllegalAccessException e) {
      throw new CopycatRuntimeException(e);
    } catch (InstantiationException e) {
      throw new CopycatRuntimeException(e);
    } catch (IOException e) {
      throw new CopycatRuntimeException(e);
    }
  }

  private boolean shouldRotate(TopicPartition topicPart) {
    return recordCounters.containsKey(topicPart) && recordCounters.get(topicPart) >= flushSize;
  }

  private void rotate(TopicPartition topicPart)
      throws CopycatException {
    try {
      RecordWriter<Long, SinkRecord> writer = writers.get(topicPart);
      writer.close();
      writers.remove(topicPart);
      commitFile(topicPart);
    } catch (IOException e) {
      throw new CopycatRuntimeException(e);
    }
  }

  private void commitFile(TopicPartition topicPart) throws IOException {
    long offset;
    offset = offsets.get(topicPart) + recordCounters.get(topicPart);
    offsets.put(topicPart, offset);
    String tempFileName = FileUtils.tempFileName(url, topicsDir, topicPart);
    String finalFileName = FileUtils.committedFileName(url, topicsDir, topicPart, offset);
    FileUtils.renameFile(fs, tempFileName, finalFileName);
  }

  private void updateRecordCounter(TopicPartition topicPart) {
    if (!recordCounters.containsKey(topicPart)) {
      recordCounters.put(topicPart, 1);
    } else {
      int count = recordCounters.get(topicPart);
      count++;
      recordCounters.put(topicPart, count);
    }
  }
}
