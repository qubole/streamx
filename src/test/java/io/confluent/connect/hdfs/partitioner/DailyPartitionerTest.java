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

package io.confluent.connect.hdfs.partitioner;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;

import static org.junit.Assert.assertEquals;

public class DailyPartitionerTest {

  private static final long partitionDurationMs = TimeUnit.HOURS.toMillis(24);

  @Test
  public void testDailyPartitioner() throws Exception {
    Map<String, Object> config = createConfig();

    DailyPartitioner partitioner = new DailyPartitioner();
    partitioner.configure(config);

    String pathFormat = partitioner.getPathFormat();
    String timeZoneString = (String) config.get(HdfsSinkConnectorConfig.TIMEZONE_CONFIG);
    long timestamp = new DateTime(2014, 2, 1, 3, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
    String encodedPartition = TimeUtils.encodeTimestamp(partitionDurationMs, pathFormat, timeZoneString, timestamp);
    String path = partitioner.generatePartitionedPath("topic", encodedPartition);
    assertEquals("topic/year=2014/month=02/day=01/", path);
  }

  private Map<String, Object> createConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put(HdfsSinkConnectorConfig.LOCALE_CONFIG, "en");
    config.put(HdfsSinkConnectorConfig.TIMEZONE_CONFIG, "America/Los_Angeles");
    return config;
  }
}
