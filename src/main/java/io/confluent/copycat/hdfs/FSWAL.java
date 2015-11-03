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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.copycat.errors.CopycatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.confluent.copycat.hdfs.WALFile.Reader;
import io.confluent.copycat.hdfs.WALFile.Writer;

public class FSWAL implements WAL {

  private static final Logger log = LoggerFactory.getLogger(FSWAL.class);
  private static final String leaseException =
      "org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException";

  private WALFile.Writer writer = null;
  private WALFile.Reader reader = null;
  private String logFile = null;
  private Configuration conf = null;
  private Storage storage;

  public FSWAL(String topicsDir, TopicPartition topicPart, Storage storage)
      throws CopycatException {

    this.storage = storage;
    this.conf = storage.conf();
    String url = storage.url();
    logFile = FileUtils.logFileName(url, topicsDir, topicPart);
  }

  @Override
  public void append(String tempFile, String committedFile) throws CopycatException {
    try {
      acquireLease();
      WALEntry key = new WALEntry(tempFile);
      WALEntry value = new WALEntry(committedFile);
      writer.append(key, value);
      writer.hsync();
    } catch (IOException e) {
      throw new CopycatException(e);
    }
  }

  public void acquireLease() throws CopycatException {
    long sleepIntervalMs = 1000L;
    long MAX_SLEEP_INTERVAL_MS = 16000L;
    while (sleepIntervalMs < MAX_SLEEP_INTERVAL_MS) {
      try {
        if (writer == null) {
          writer = WALFile.createWriter(conf, Writer.file(new Path(logFile)),
                                        Writer.appendIfExists(true));
        }
        log.info("Successfully acquired lease for {}", logFile);
        break;
      } catch (RemoteException e) {
        if (e.getClassName().equals(leaseException)) {
          log.info("Cannot acquire lease on FSWAL {}", logFile);
          try {
            Thread.sleep(sleepIntervalMs);
          } catch (InterruptedException ie) {
            throw new CopycatException(ie);
          }
          sleepIntervalMs = sleepIntervalMs * 2;
        } else {
          throw new CopycatException(e);
        }
      } catch (IOException e) {
        throw new CopycatException("Error creating writer for log file " + logFile, e);
      }
    }
    if (sleepIntervalMs >= MAX_SLEEP_INTERVAL_MS) {
      throw new CopycatException("Cannot acquire lease after timeout, will retry.");
    }
  }

  @Override
  public void apply() throws CopycatException {
    try {
      if (!storage.exists(logFile)) {
        return;
      }
      acquireLease();
      if (reader == null) {
        reader = new WALFile.Reader(conf, Reader.file(new Path(logFile)));
      }
      WALEntry key = new WALEntry();
      WALEntry value = new WALEntry();
      while (reader.next(key, value)) {
        String tempFile = key.getFilename();
        String committedFile = value.getFilename();
        if (!storage.exists(committedFile)) {
          storage.commit(tempFile, committedFile);
        }
      }
    } catch (IOException e) {
      throw new CopycatException(e);
    }
  }

  @Override
  public void truncate() throws CopycatException {
    try {
      String oldLogFile = logFile + ".1";
      storage.delete(oldLogFile);
      storage.commit(logFile, oldLogFile);
    } catch (IOException e) {
      throw new CopycatException(e);
    }
  }

  @Override
  public void close() throws CopycatException {
    try {
      if (writer != null) {
        writer.hsync();
        writer.close();
      }
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      throw new CopycatException("Error closing " + logFile, e);
    }
  }

  @Override
  public String getLogFile() {
    return logFile;
  }
}
