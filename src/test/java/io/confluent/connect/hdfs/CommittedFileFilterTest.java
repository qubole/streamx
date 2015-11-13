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

package io.confluent.connect.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommittedFileFilterTest {
  private static FileSystem fs;
  private static Path ROOT_PATH = new Path(System.getProperty(
      "test.build.data", "build/test/data"));

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    fs = FileSystem.get(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    fs.close();
  }

  @Test
  public void testCommitedFileFilter() throws IOException {

    Path log = new Path(ROOT_PATH, "log");
    Path oldLog = new Path(ROOT_PATH, "log.1");

    String temp_name = UUID.randomUUID().toString() + "_" + "tmp";
    Path tmp = new Path(ROOT_PATH, temp_name);

    Path valid1 = new Path(ROOT_PATH, "a_b");
    Path valid2 = new Path(ROOT_PATH, "a_1");
    Path valid3 = new Path(ROOT_PATH, "1_b");
    Path valid4 = new Path(ROOT_PATH, "1_2");

    Path invalid1 = new Path(ROOT_PATH, "1_2_3");
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

    fs.createNewFile(invalid1);
    fs.createNewFile(invalid2);
    fs.createNewFile(invalid3);
    fs.createNewFile(invalid4);

    FileStatus[] statuses = fs.listStatus(ROOT_PATH, new CommittedFileFilter());
    Set<String> files = new HashSet<>();
    for (FileStatus status : statuses) {
      files.add(status.getPath().getName());
    }

    assertEquals(files.size(), 4);
    assertTrue(files.contains(valid1.getName()));
    assertTrue(files.contains(valid2.getName()));
    assertTrue(files.contains(valid3.getName()));
    assertTrue(files.contains(valid4.getName()));
    fs.deleteOnExit(ROOT_PATH);
  }
}
