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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommittedFileFilterTest {
  private static FileSystem fs;
  private static Path ROOT_PATH = new Path(System.getProperty("test.build.data", "build/test/data"));

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    fs = FileSystem.get(conf);
  }

  @After
  public void tearDown() throws Exception {
    fs.deleteOnExit(ROOT_PATH);
    if (fs != null) {
      fs.close();
    }
  }

  @Test
  public void testTopicPartitionCommittedFileFilter() throws Exception {
    Path log = new Path(ROOT_PATH, "log");
    Path oldLog = new Path(ROOT_PATH, "log.1");
    String tempName = UUID.randomUUID().toString() + "_" + "tmp";
    Path tmp = new Path(ROOT_PATH, tempName);
    Path valid1 = new Path(ROOT_PATH, "topic+1+2+3.abc");
    Path valid2 = new Path(ROOT_PATH, "topic+1+55+67.def");
    Path validOtherTopic = new Path(ROOT_PATH, "namespace.topic+1+55+67.def");
    Path invalid1 = new Path(ROOT_PATH, "1+2+3");
    Path invalid2 = new Path(ROOT_PATH, "a_b_c_d");
    Path invalid3 = new Path(ROOT_PATH, "tmp");
    Path invalid4 = new Path(ROOT_PATH, "a.b");
    Path invalid5 = new Path(ROOT_PATH, "topic_234_56_78.ghi");
    Path invalid6 = new Path(ROOT_PATH, "topic_34_56_7.hjk");

    fs.createNewFile(log);
    fs.createNewFile(oldLog);
    fs.createNewFile(tmp);
    fs.createNewFile(valid1);
    fs.createNewFile(valid2);
    fs.createNewFile(validOtherTopic);
    fs.createNewFile(invalid1);
    fs.createNewFile(invalid2);
    fs.createNewFile(invalid3);
    fs.createNewFile(invalid4);
    fs.createNewFile(invalid5);
    fs.createNewFile(invalid6);

    TopicPartition tp = new TopicPartition("topic", 1);
    FileStatus[] statuses = fs.listStatus(ROOT_PATH, new TopicPartitionCommittedFileFilter(tp));
    Set<String> files = new HashSet<>();
    for (FileStatus status : statuses) {
      files.add(status.getPath().getName());
    }

    assertEquals(2, files.size());
    assertTrue(files.contains(valid1.getName()));
    assertTrue(files.contains(valid2.getName()));

    TopicPartition tp2 = new TopicPartition("namespace.topic", 1);
    FileStatus[] statusesOtherTopic = fs.listStatus(
            ROOT_PATH, new TopicPartitionCommittedFileFilter(tp2));
    assertEquals(1, statusesOtherTopic.length);
    assertEquals(validOtherTopic.getName(), statusesOtherTopic[0].getPath().getName());

    fs.deleteOnExit(ROOT_PATH);
  }

  @Test
  public void testTopicCommittedFileFilter() throws Exception {
    Path log = new Path(ROOT_PATH, "log");
    Path oldLog = new Path(ROOT_PATH, "log.1");
    String tempName = UUID.randomUUID().toString() + "_" + "tmp";
    Path tmp = new Path(ROOT_PATH, tempName);
    Path valid1 = new Path(ROOT_PATH, "topic+1+2+3.abc");
    Path valid2 = new Path(ROOT_PATH, "topic+1+55+67.def");
    Path valid3 = new Path(ROOT_PATH, "topic+234+56+78.ghi");
    Path valid4 = new Path(ROOT_PATH, "topic+34+56+7.hjk");
    Path validOtherTopic = new Path(ROOT_PATH, "namespace.topic+34+56+7.hjk");
    Path invalid1 = new Path(ROOT_PATH, "1+2+3");
    Path invalid2 = new Path(ROOT_PATH, "a_b_c_d");
    Path invalid3 = new Path(ROOT_PATH, "tmp");
    Path invalid4 = new Path(ROOT_PATH, "a.b");

    fs.createNewFile(log);
    fs.createNewFile(oldLog);
    fs.createNewFile(tmp);
    fs.createNewFile(valid1);
    fs.createNewFile(valid2);
    fs.createNewFile(valid3);
    fs.createNewFile(valid4);
    fs.createNewFile(validOtherTopic);
    fs.createNewFile(invalid1);
    fs.createNewFile(invalid2);
    fs.createNewFile(invalid3);
    fs.createNewFile(invalid4);

    FileStatus[] statuses = fs.listStatus(ROOT_PATH, new TopicCommittedFileFilter("topic"));
    Set<String> files = new HashSet<>();
    for (FileStatus status : statuses) {
      files.add(status.getPath().getName());
    }

    assertEquals(4, files.size());
    assertTrue(files.contains(valid1.getName()));
    assertTrue(files.contains(valid2.getName()));
    assertTrue(files.contains(valid3.getName()));
    assertTrue(files.contains(valid4.getName()));

    FileStatus[] statusesOtherTopic = fs.listStatus(
            ROOT_PATH, new TopicCommittedFileFilter("namespace.topic"));
    assertEquals(1, statusesOtherTopic.length);
    assertEquals(validOtherTopic.getName(), statusesOtherTopic[0].getPath().getName());

    fs.deleteOnExit(ROOT_PATH);
  }

  @Test
  public void testCommittedFileFilter() throws Exception {
    Path valid1 = new Path(ROOT_PATH, "__234+1+2+3.abc");
    Path valid2 = new Path(ROOT_PATH, "456+1+55+67.def");
    Path valid3 = new Path(ROOT_PATH, "topic+234+56+78.ghi");
    Path valid4 = new Path(ROOT_PATH, "topic+34+56+7.hjk");
    Path valid5 = new Path(ROOT_PATH, "--._+34+56+7.hjk");
    Path valid6 = new Path(ROOT_PATH, "topic+234+56+78");
    Path invalid1 = new Path(ROOT_PATH, "topic+234+56+78.");

    fs.createNewFile(valid1);
    fs.createNewFile(valid2);
    fs.createNewFile(valid3);
    fs.createNewFile(valid4);
    fs.createNewFile(valid5);
    fs.createNewFile(valid6);
    fs.createNewFile(invalid1);

    FileStatus[] statuses = fs.listStatus(ROOT_PATH, new CommittedFileFilter());
    Set<String> files = new HashSet<>();
    for (FileStatus status : statuses) {
      files.add(status.getPath().getName());
    }

    assertEquals(6, files.size());
    assertTrue(files.contains(valid1.getName()));
    assertTrue(files.contains(valid2.getName()));
    assertTrue(files.contains(valid3.getName()));
    assertTrue(files.contains(valid4.getName()));
    assertTrue(files.contains(valid5.getName()));
    assertTrue(files.contains(valid6.getName()));

    fs.deleteOnExit(ROOT_PATH);
  }
}
