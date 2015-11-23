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

package io.confluent.connect.hdfs.utils;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.hdfs.wal.WAL;

public class MemoryWAL implements WAL {

  private String logFile;
  private Storage storage;
  private static Map<String, List<Object>> data = Data.getData();

  public MemoryWAL(String topicsDir, TopicPartition topicPart, Storage storage)
      throws ConnectException {
    this.storage = storage;
    String url = storage.url();
    logFile = FileUtils.logFileName(url, topicsDir, topicPart);
  }


  @Override
  public void acquireLease() throws ConnectException {

  }

  @Override
  public void append(String tempFile, String committedFile) throws ConnectException {
    try {
      LogEntry entry = new LogEntry(tempFile, committedFile);
      storage.append(logFile, entry);
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public void apply() throws ConnectException {
    try {
      if (data.containsKey(logFile)) {
        List<Object> entryList = data.get(logFile);
        for (Object entry : entryList) {
          LogEntry logEntry = (LogEntry) entry;
          storage.commit(logEntry.key(), logEntry.value());
        }
      }
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public void truncate() throws ConnectException {
    try {
      storage.commit(logFile, logFile + ".1");
      storage.delete(logFile);
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public void close() throws ConnectException {
    try {
      storage.close();
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public String getLogFile() {
    return logFile;
  }

  private static class LogEntry {
    private String key;
    private String value;

    public LogEntry(String key, String value) {
      this.key = key;
      this.value = value;
    }

    public String key() {
      return key;
    }

    public String value() {
      return value;
    }
  }
}
