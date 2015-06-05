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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import io.confluent.copycat.connector.TopicPartition;

public class FileUtils {

  public static void renameFile(final FileSystem fs, String sourcePath,
                          String targetPath) throws IOException {
    if (sourcePath.equals(targetPath)) {
      return;
    }
    final Path srcPath = new Path(sourcePath);
    final Path dstPath = new Path(targetPath);
    if (fs.exists(srcPath)) {
      fs.rename(srcPath, dstPath);
    }
  }

  public static String tempFileName(String url, String topicsDir, TopicPartition topicPart) {
    return fileName(url, topicsDir, topicPart, "tmp");
  }

  public static String committedFileName(String url, String topicsDir, TopicPartition topicPart,
                                         long offset) {
    String suffix = String.valueOf(offset);
    return fileName(url, topicsDir, topicPart, suffix);
  }

  public static String fileName(String url, String topicsDir, TopicPartition topicPart, String suffix) {
    String topic = topicPart.topic();
    int partition = topicPart.partition();
    return url + "/" + topicsDir + "/" + topic + "/" + partition + "." + suffix;
  }

}
