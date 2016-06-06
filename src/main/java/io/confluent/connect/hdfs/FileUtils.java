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

package io.confluent.connect.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.regex.Matcher;

import io.confluent.connect.hdfs.filter.CommittedFileFilter;
import io.confluent.connect.hdfs.storage.Storage;

public class FileUtils {
  private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

  public static String logFileName(String url, String logsDir, TopicPartition topicPart) {
    return fileName(url, logsDir, topicPart, "log");
  }

  public static String directoryName(String url, String topicsDir, TopicPartition topicPart) {
    String topic = topicPart.topic();
    int partition = topicPart.partition();
    return url + "/" + topicsDir + "/" + topic + "/" + partition;
  }

  public static String fileName(String url, String topicsDir, TopicPartition topicPart,
                                String name) {
    String topic = topicPart.topic();
    int partition = topicPart.partition();
    return url + "/" + topicsDir + "/" + topic + "/" + partition + "/" + name;
  }

  public static String hiveDirectoryName(String url, String topicsDir, String topic) {
    return url + "/" + topicsDir + "/" + topic + "/";
  }

  public static String fileName(String url, String topicsDir, String directory, String name) {
    return url + "/" + topicsDir + "/" + directory + "/" + name;
  }

  public static String directoryName(String url, String topicsDir, String directory) {
    return url + "/" + topicsDir + "/" + directory;
  }

  public static String tempFileName(String url, String topicsDir, String directory,
                                    String extension) {
    UUID id = UUID.randomUUID();
    String name = id.toString() + "_" + "tmp" + extension;
    return fileName(url, topicsDir, directory, name);
  }

  public static String committedFileName(String url, String topicsDir, String directory,
                                         TopicPartition topicPart, long startOffset, long endOffset,
                                         String extension, String zeroPadFormat) {
    String topic = topicPart.topic();
    int partition = topicPart.partition();
    StringBuilder sb = new StringBuilder();
    sb.append(topic);
    sb.append(HdfsSinkConnectorConstants.COMMMITTED_FILENAME_SEPARATOR);
    sb.append(partition);
    sb.append(HdfsSinkConnectorConstants.COMMMITTED_FILENAME_SEPARATOR);
    sb.append(String.format(zeroPadFormat, startOffset));
    sb.append(HdfsSinkConnectorConstants.COMMMITTED_FILENAME_SEPARATOR);
    sb.append(String.format(zeroPadFormat, endOffset));
    sb.append(extension);
    String name = sb.toString();
    return fileName(url, topicsDir, directory, name);
  }

  public static String topicDirectory(String url, String topicsDir, String topic) {
    return url + "/" + topicsDir + "/" + topic;
  }

  private static ArrayList<FileStatus> traverseImpl(Storage storage, Path path, PathFilter filter)
      throws IOException {
    if (!storage.exists(path.toString())) {
      return new ArrayList<>();
    }
    ArrayList<FileStatus> result = new ArrayList<>();
    FileStatus[] statuses = storage.listStatus(path.toString());
    for (FileStatus status : statuses) {
      if (status.isDirectory()) {
        result.addAll(traverseImpl(storage, status.getPath(), filter));
      } else {
        if (filter.accept(status.getPath())) {
          result.add(status);
        }
      }
    }
    return result;
  }

  public static FileStatus[] traverse(Storage storage, Path path, PathFilter filter)
      throws IOException {
    ArrayList<FileStatus> result = traverseImpl(storage, path, filter);
    return result.toArray(new FileStatus[result.size()]);
  }

  public static FileStatus fileStatusWithMaxOffset(Storage storage, Path path,
                                                   CommittedFileFilter filter) throws IOException {
    if (!storage.exists(path.toString())) {
      return null;
    }
    long maxOffset = -1L;
    FileStatus fileStatusWithMaxOffset = null;
    FileStatus[] statuses = storage.listStatus(path.toString());
    for (FileStatus status : statuses) {
      if (status.isDirectory()) {
        FileStatus fileStatus = fileStatusWithMaxOffset(storage, status.getPath(), filter);
        if (fileStatus != null) {
          long offset = extractOffset(fileStatus.getPath().getName());
          if (offset > maxOffset) {
            maxOffset = offset;
            fileStatusWithMaxOffset = fileStatus;
          }
        }
      } else {
        String filename = status.getPath().getName();
        log.trace("Checked for max offset: {}", status.getPath());
        if (filter.accept(status.getPath())) {
          long offset = extractOffset(filename);
          if (offset > maxOffset) {
            maxOffset = offset;
            fileStatusWithMaxOffset = status;
          }
        }
      }
    }
    return fileStatusWithMaxOffset;
  }

  public static long extractOffset(String filename) {
    Matcher m = HdfsSinkConnectorConstants.COMMITTED_FILENAME_PATTERN.matcher(filename);
    // NB: if statement has side effect of enabling group() call
    if (!m.matches()) {
      throw new IllegalArgumentException(filename + " does not match COMMITTED_FILENAME_PATTERN");
    }
    return Long.parseLong(m.group(HdfsSinkConnectorConstants.PATTERN_END_OFFSET_GROUP));
  }

  private static ArrayList<FileStatus> getDirectoriesImpl(Storage storage, Path path)
      throws IOException {
    FileStatus[] statuses = storage.listStatus(path.toString());
    ArrayList<FileStatus> result = new ArrayList<>();
    for (FileStatus status : statuses) {
      if (status.isDirectory()) {
        int count = 0;
        FileStatus[] fileStatuses = storage.listStatus(status.getPath().toString());
        for (FileStatus fileStatus : fileStatuses) {
          if (fileStatus.isDirectory()) {
            result.addAll(getDirectoriesImpl(storage, fileStatus.getPath()));
          } else {
            count++;
          }
        }
        if (count == fileStatuses.length) {
          result.add(status);
        }
      }
    }
    return result;
  }

  public static FileStatus[] getDirectories(Storage storage, Path path) throws IOException {
    ArrayList<FileStatus> result = getDirectoriesImpl(storage, path);
    return result.toArray(new FileStatus[result.size()]);
  }

  private static ArrayList<FileStatus> traverseImpl(FileSystem fs, Path path) throws IOException {
    if (!fs.exists(path)) {
      return new ArrayList<>();
    }
    ArrayList<FileStatus> result = new ArrayList<>();
    FileStatus[] statuses = fs.listStatus(path);
    for (FileStatus status : statuses) {
      if (status.isDirectory()) {
        result.addAll(traverseImpl(fs, status.getPath()));
      } else {
        result.add(status);
      }
    }
    return result;
  }

  public static FileStatus[] traverse(FileSystem fs, Path path) throws IOException {
    ArrayList<FileStatus> result = traverseImpl(fs, path);
    return result.toArray(new FileStatus[result.size()]);
  }
}
