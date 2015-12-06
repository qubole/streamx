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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.confluent.common.config.ConfigException;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;

public class TimeBasedPartitioner implements Partitioner {

  // Duration of a partition in milliseconds.
  private long partitionDurationMs;
  private DateTimeFormatter formatter;
  protected List<FieldSchema> partitionFields = new ArrayList<>();
  private static String patternString = "'year'=Y{1,5}/('month'=M{1,5}/)?('day'=d{1,3}/)?('hour'=H{1,3}/)?('minute'=m{1,3}/)?";
  private static Pattern pattern = Pattern.compile(patternString);

  protected void init(long partitionDurationMs, String pathFormat, Locale locale,
                      DateTimeZone timeZone, boolean hiveIntegration) {
    this.partitionDurationMs = partitionDurationMs;
    this.formatter = getDateTimeFormatter(pathFormat, timeZone).withLocale(locale);
    addToPartitionFields(pathFormat, hiveIntegration);
  }

  private static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
    return DateTimeFormat.forPattern(str).withZone(timeZone);
  }

  public static long getPartition(long timeGranularityMs, long timestamp, DateTimeZone timeZone) {
    long adjustedTimeStamp = timeZone.convertUTCToLocal(timestamp);
    long partitionedTime = (adjustedTimeStamp / timeGranularityMs) * timeGranularityMs;
    return timeZone.convertLocalToUTC(partitionedTime, false);
  }

  @Override
  public void configure(Map<String, Object> config) {
    long partitionDurationMs = (long) config.get(HdfsSinkConnectorConfig.PARTITION_DURATION_MS_CONFIG);
    if (partitionDurationMs < 0) {
      throw new ConfigException(HdfsSinkConnectorConfig.PARTITION_DURATION_MS_CONFIG,
                                partitionDurationMs, "Partition duration needs to be a positive.");
    }

    String pathFormat = (String) config.get(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG);
    if (pathFormat.equals("")) {
      throw new ConfigException(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG,
                                pathFormat, "Path format cannot be empty.");
    }

    String localeString = (String) config.get(HdfsSinkConnectorConfig.LOCALE_CONFIG);
    if (localeString.equals("")) {
      throw new ConfigException(HdfsSinkConnectorConfig.LOCALE_CONFIG,
                                localeString, "Locale cannot be empty.");
    }
    String timeZoneString = (String) config.get(HdfsSinkConnectorConfig.TIMEZONE_CONFIG);
    if (timeZoneString.equals("")) {
      throw new ConfigException(HdfsSinkConnectorConfig.TIMEZONE_CONFIG,
                                timeZoneString, "Timezone cannot be empty.");
    }

    String hiveIntString = (String) config.get(HdfsSinkConnectorConfig.HIVE_INTEGRATION_CONFIG);
    boolean hiveIntegration = hiveIntString != null && hiveIntString.toLowerCase().equals("true");

    Locale locale = new Locale(localeString);
    DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
    init(partitionDurationMs, pathFormat, locale, timeZone, hiveIntegration);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    long timestamp = System.currentTimeMillis();
    DateTime bucket = new DateTime(getPartition(partitionDurationMs, timestamp, formatter.getZone()));
    return bucket.toString(formatter);
  }


  @Override
  public String generatePartitionedPath(String topic, String encodedPartition) {
    return topic + "/" + encodedPartition;
  }

  @Override
  public List<FieldSchema> partitionFields() {
    return partitionFields;
  }

  private boolean verifyDateTimeFormat(String pathFormat) {
    Matcher m = pattern.matcher(pathFormat);
    return m.matches();
  }

  private void addToPartitionFields(String pathFormat, boolean hiveIntegration) {
    if (hiveIntegration && !verifyDateTimeFormat(pathFormat)) {
      throw new ConfigException(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG, pathFormat,
                                "Path format doesn't meet the requirements for Hive integration, "
                                + "which require prefixing each DateTime component with its name.");
    }
    for (String field: pathFormat.split("/")) {
      String[] parts = field.split("=");
      FieldSchema fieldSchema = new FieldSchema(parts[0].replace("'", ""), TypeInfoFactory.stringTypeInfo.toString(), "");
      partitionFields.add(fieldSchema);
    }
  }
}
