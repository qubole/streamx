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

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import io.confluent.connect.avro.AvroData;

public class HdfsWriter {

  private static final Logger log = LoggerFactory.getLogger(HdfsWriter.class);
  private Map<TopicPartition, RecordWriter<Long, SinkRecord>> writers = null;
  private Map<TopicPartition, WAL> wals = null;
  private Map<TopicPartition, String> tempFileNames = null;
  private Map<TopicPartition, Long> offsets = null;
  private Map<TopicPartition, Integer> recordCounters = null;
  private Configuration conf;
  private RecordWriterProvider writerProvider;
  private String url;
  private int flushSize;
  private String topicsDir;
  private Set<TopicPartition> assignment;
  private AvroData avroData;
  private SinkTaskContext context;
  private Map<TopicPartition, State> states;
  private Set<TopicPartition> recovered;
  private Map<TopicPartition, Queue<SinkRecord>> buffer;
  private Storage storage;
  private long backOffMs;
  private Set<TopicPartition> lastAssignment;
  private Map<TopicPartition, Long> failureTime;
  private Map<TopicPartition, Schema> schemas;
  private final Compatibility compatibility;

  private enum State {
    RECOVERY_STARTED,
    RECOVERY_PARTITION_PAUSED,
    WAL_CREATED,
    WAL_APPLIED,
    WAL_TRUNCATED,
    OFFSET_RESET,
    WRITE_STARTED,
    WRITE_PARTITION_PAUSED,
    SHOULD_ROTATE,
    TEMP_FILE_CLOSED,
    WAL_APPENDED,
    FILE_COMMITTED;

    private static State[] vals = values();
    public State next() {
      return vals[(this.ordinal() + 1) % vals.length];
    }
  }

  private enum Compatibility {
    NONE,
    BACKWARD,
    FORWARD,
    FULL
  }

  @SuppressWarnings("unchecked")
  public HdfsWriter(HdfsSinkConnectorConfig connectorConfig, SinkTaskContext context, AvroData avroData) {
    try {
      writers = new HashMap<>();
      wals = new HashMap<>();
      offsets = new HashMap<>();
      recordCounters = new HashMap<>();
      tempFileNames = new HashMap<>();

      flushSize = connectorConfig.getInt(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG);
      backOffMs = connectorConfig.getLong(HdfsSinkConnectorConfig.RETRY_BACKOFF_CONFIG);
      url = connectorConfig.getString(HdfsSinkConnectorConfig.HDFS_URL_CONFIG);
      topicsDir = connectorConfig.getString(HdfsSinkConnectorConfig.TOPIC_DIR_CONFIG);
      conf = new Configuration();

      Class<? extends Storage> storageClass =
          (Class<? extends Storage>) Class.forName(connectorConfig.getString(HdfsSinkConnectorConfig.STORAGE_CLASS_CONFIG));
      this.storage = StorageFactory.createStorage(storageClass, conf, url);
      createTopicsDir();
      writerProvider = ((Class<RecordWriterProvider>) Class.forName(connectorConfig.getString(
              HdfsSinkConnectorConfig.RECORD_WRITER_PROVIDER_CLASS_CONFIG))).newInstance();

      this.context = context;
      this.assignment = context.assignment();
      this.avroData = avroData;

      states = new HashMap<>();
      for (TopicPartition tp: assignment) {
        states.put(tp, State.RECOVERY_STARTED);
      }
      buffer = new HashMap<>();
      for (TopicPartition tp: assignment) {
        buffer.put(tp, new LinkedList<SinkRecord>());
      }
      recovered = new HashSet<>();
      lastAssignment = new HashSet<>();
      failureTime = new HashMap<>();
      schemas = new HashMap<>();
      compatibility = getCompatibility(connectorConfig.getString(HdfsSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG));
    } catch (IOException e) {
      throw new ConnectException(e);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new ConnectException("Reflection exception: ", e);
    }
  }

  /**
   * @return get committed offsets of previous task runs.
   */
  public Map<TopicPartition, Long> getCommittedOffsets() {
    return offsets;
  }

  public void write(Collection<SinkRecord> records) throws IOException {
    String topic;
    int partition;
    for (SinkRecord record: records) {
      topic = record.topic();
      partition = record.kafkaPartition();
      TopicPartition topicPart = new TopicPartition(topic, partition);
      buffer.get(topicPart).add(record);
    }
    for (TopicPartition topicPart: assignment) {
      if (failureTime.containsKey(topicPart)) {
        long now = System.currentTimeMillis();
        if (now - failureTime.get(topicPart) < backOffMs) {
          continue;
        }
        failureTime.remove(topicPart);
      }
      execute(topicPart);
    }
  }
  
  public void recover(TopicPartition topicPart) {
    try {
      switch (states.get(topicPart)) {
        case RECOVERY_STARTED:
          pause(topicPart);
          nextState(topicPart);
        case RECOVERY_PARTITION_PAUSED:
          createWAL(topicPart);
          nextState(topicPart);
        case WAL_CREATED:
          applyWAL(topicPart);
          nextState(topicPart);
        case WAL_APPLIED:
          truncateWAL(topicPart);
          nextState(topicPart);
        case WAL_TRUNCATED:
          resetOffsets(topicPart);
          nextState(topicPart);
        case OFFSET_RESET:
          resume(topicPart);
          nextState(topicPart);
          break;
        default:
          log.error("{} is not a valid state to perform recovery.", states.get(topicPart));
      }
    } catch (ConnectException e) {
      log.error("Recovery failed.");
      recordFailureTime(topicPart);
      setRetryBackoff(backOffMs);
    }
  }

  private void execute(TopicPartition topicPart) throws IOException {
    Queue<SinkRecord> records = buffer.get(topicPart);
    if (states.get(topicPart).compareTo(State.WRITE_STARTED) < 0) {
      recover(topicPart);
    }
    while(!records.isEmpty()) {
      try {
        switch (states.get(topicPart)) {
          case WRITE_STARTED:
            pause(topicPart);
            nextState(topicPart);
          case WRITE_PARTITION_PAUSED:
            if (!schemas.containsKey(topicPart)) {
              getLastUsedSchema(topicPart);
            }
            SinkRecord record = records.peek();
            Schema valueSchema = record.valueSchema();
            if (shouldRotate(topicPart)) {
              nextState(topicPart);
            } else if (shouldChangeSchema(topicPart, valueSchema)) {
              schemas.put(topicPart, valueSchema);
              if (recordCounters.containsKey(topicPart) && recordCounters.get(topicPart) > 0) {
                nextState(topicPart);
              } else {
                break;
              }
            } else {
              SinkRecord projectedRecord = project(topicPart, record);
              writeRecord(topicPart, projectedRecord);
              records.poll();
              break;
            }
          case SHOULD_ROTATE:
            closeTempfile(topicPart);
            nextState(topicPart);
          case TEMP_FILE_CLOSED:
            appendToWAL(topicPart);
            nextState(topicPart);
          case WAL_APPENDED:
            commitFile(topicPart);
            nextState(topicPart);
          case FILE_COMMITTED:
            setState(topicPart, State.WRITE_PARTITION_PAUSED);
            break;
          default:
            log.error("{} is not a valid state to write record.", states.get(topicPart));
        }
      } catch (IllegalWorkerStateException e) {
        // Should we retry in this case?
        throw e;
      } catch (SchemaProjectorException e) {
        throw new RuntimeException(e);
      } catch (IOException | ConnectException e) {
        log.error("Exception on {}", topicPart);
        recordFailureTime(topicPart);
        setRetryBackoff(backOffMs);
        break;
      }
    }
    if (records.isEmpty()) {
      resume(topicPart);
      setState(topicPart, State.WRITE_STARTED);
    }
  }

  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    assignment.clear();
    assignment.addAll(partitions);

    // handle partitions that no longer assigned to the task
    for (TopicPartition topicPart: lastAssignment) {
      if (!assignment.contains(topicPart)) {
        try {
          if (writers.containsKey(topicPart)) {
            closeTempfile(topicPart);
            appendToWAL(topicPart);
            commitFile(topicPart);
          }
        } catch (IOException e) {
          log.error("Error rotating {} when closing task.", tempFileNames.get(topicPart));
          if (writers.containsKey(topicPart)) {
            writers.remove(topicPart);
          }
        }

        WAL wal = null;
        try {
          if (wals.containsKey(topicPart)) {
            wal = wals.get(topicPart);
            wal.close();
            wals.remove(topicPart);
          }
        } catch (ConnectException e) {
          log.error("Error closing {}.", wal.getLogFile());
          if (wals.containsKey(topicPart)){
            wals.remove(topicPart);
          }
        }

        buffer.remove(topicPart);
        states.remove(topicPart);
        tempFileNames.remove(topicPart);
        offsets.remove(topicPart);
        recordCounters.remove(topicPart);
        failureTime.remove(topicPart);
        if (recovered.contains(topicPart)) {
          recovered.remove(topicPart);
        }
      }
    }

    // handle new partitions
    for (TopicPartition topicPart: assignment) {
      if (!lastAssignment.contains(topicPart)) {
        states.put(topicPart, State.RECOVERY_STARTED);
        buffer.put(topicPart, new LinkedList<SinkRecord>());
      }
    }
  }

  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    lastAssignment.clear();
    lastAssignment.addAll(partitions);
  }

  private void nextState(TopicPartition topicPart) {
    State curState = states.get(topicPart);
    states.put(topicPart, curState.next());
  }

  private void setState(TopicPartition topicPart, State state) {
    states.put(topicPart, state);
  }

  private void recordFailureTime(TopicPartition topicPart) {
    long time = System.currentTimeMillis();
    failureTime.put(topicPart, time);
  }

  private void createTopicsDir() throws IOException {
    String dir = url + "/" + topicsDir;
    if (!storage.exists(dir)) {
      storage.mkdirs(dir);
    }
  }

  private Compatibility getCompatibility(String compatibilityString) {
    switch (compatibilityString) {
      case "BACKWARD":
        return Compatibility.BACKWARD;
      case "FORWARD":
        return Compatibility.FORWARD;
      case "FULL":
        return Compatibility.FULL;
      default:
        return Compatibility.NONE;
    }
  }

  private Schema getSchema(TopicPartition topicPart) {
    return schemas.get(topicPart);
  }

  private SinkRecord project(TopicPartition topicPart, SinkRecord record) {
    switch (compatibility) {
      case BACKWARD:
      case FULL:
      case FORWARD:
        Schema valueSchema = record.valueSchema();
        Object value = record.value();
        Schema currentSchema = schemas.get(topicPart);
        if (valueSchema.equals(currentSchema)) {
          return record;
        }
        Object projected = SchemaProjector.project(valueSchema, value, currentSchema);
        return new SinkRecord(record.topic(), record.kafkaPartition(), record.keySchema(),
                            record.key(), currentSchema, projected, record.kafkaOffset());
      default:
        return record;
    }
  }

  private void readOffsets(TopicPartition topicPart) throws ConnectException {
    String path = FileUtils.directoryName(url, topicsDir, topicPart);
    PathFilter filter = new CommittedFileFilter();
    try {
      if (!storage.exists(path)) {
        return;
      }
      FileStatus[] committedFiles = storage.listStatus(path, filter);
      for (FileStatus committedFile : committedFiles) {
        String filename = committedFile.getPath().getName();
        String[] parts = filename.split("_");
        try {
          long endOffset = Long.parseLong(parts[1]);
          if (!offsets.containsKey(topicPart) || endOffset > offsets.get(topicPart)) {
            offsets.put(topicPart, endOffset);
          }
        } catch (NumberFormatException e) {
          log.warn("Invalid committed file: {}", filename);
        }
      }
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  public RecordWriter<Long, SinkRecord> getWriter(TopicPartition topicPart, SinkRecord record)
      throws ConnectException {
    try {
      if (writers.containsKey(topicPart)) {
        return writers.get(topicPart);
      }
      String fileName = FileUtils.tempFileName(url, topicsDir, topicPart);
      RecordWriter<Long, SinkRecord> writer = writerProvider.getRecordWriter(conf, fileName, record, avroData);
      writers.put(topicPart, writer);
      tempFileNames.put(topicPart, fileName);
      return writer;
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  private boolean shouldRotate(TopicPartition topicPart) {
    return recordCounters.containsKey(topicPart) && recordCounters.get(topicPart) >= flushSize;
  }


  private boolean shouldChangeSchema(TopicPartition topicPart, Schema valueSchema) {
    Schema currentSchema = getSchema(topicPart);
    if (currentSchema == null) {
      return true;
    }
    if ((valueSchema.version() == null || currentSchema.version() == null) && compatibility != Compatibility.NONE) {
      throw new SchemaProjectorException("Schema version required for " + compatibility.toString() + " compatibility");
    }
    switch (compatibility) {
      case BACKWARD:
      case FULL:
        return (valueSchema.version()).compareTo(currentSchema.version()) > 0;
      case FORWARD:
        return (valueSchema.version()).compareTo(currentSchema.version()) < 0;
      default:
        return !valueSchema.equals(currentSchema);
    }
  }

  public void close() throws ConnectException {
    List<Exception> exceptions = new ArrayList<>();
    for (TopicPartition topicPart : assignment) {
      try {
        if (writers.containsKey(topicPart)) {
          closeTempfile(topicPart);
          appendToWAL(topicPart);
          commitFile(topicPart);
        }
      } catch (IOException e) {
        log.error("Error rotating {} when closing task.", tempFileNames.get(topicPart));
        exceptions.add(e);
      }
    }
    writers.clear();

    for (WAL wal : wals.values()) {
      try {
        wal.close();
      } catch (ConnectException e) {
        log.error("Error closing {}.", wal.getLogFile());
        exceptions.add(e);
      }
    }
    wals.clear();

    tempFileNames.clear();
    recordCounters.clear();
    offsets.clear();
    lastAssignment.clear();
    failureTime.clear();

    try {
      storage.close();
    } catch (IOException e) {
      log.error("Error closing storage {}.", storage.url());
      exceptions.add(e);
    }

    if (exceptions.size() != 0) {
      StringBuilder sb = new StringBuilder();
      for (Exception exception: exceptions) {
        sb.append(exception.getMessage());
        sb.append("\n");
      }
      throw new ConnectException("Error closing writer: " + sb.toString());
    }
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

  private void pause(TopicPartition topicPart) {
    context.pause(topicPart);
  }

  private void resume(TopicPartition topicPart) {
    context.resume(topicPart);
  }

  private void createWAL(TopicPartition topicPart) throws ConnectException {
    if (!wals.containsKey(topicPart)) {
      WAL wal = storage.wal(topicsDir, topicPart);
      wals.put(topicPart, wal);
    }
  }

  private void applyWAL(TopicPartition topicPart) throws ConnectException {
    if (!recovered.contains(topicPart)) {
      WAL wal = wals.get(topicPart);
      wal.apply();
    }
  }

  private void truncateWAL(TopicPartition topicPart) throws ConnectException {
    if (!recovered.contains(topicPart)) {
      WAL wal = wals.get(topicPart);
      wal.truncate();
    }
  }

  private void resetOffsets(TopicPartition topicPart) throws ConnectException {
    if (!recovered.contains(topicPart)) {
      readOffsets(topicPart);
      if (offsets.containsKey(topicPart)) {
        long offset = offsets.get(topicPart);
        context.offset(topicPart, offset);
      }
      recovered.add(topicPart);
    }
  }

  private void writeRecord(TopicPartition topicPart, SinkRecord record) throws IOException {
    RecordWriter<Long, SinkRecord> writer = getWriter(topicPart, record);
    writer.write(System.currentTimeMillis(), record);
    if (!offsets.containsKey(topicPart)) {
      offsets.put(topicPart, record.kafkaOffset() - 1);
    }
    updateRecordCounter(topicPart);
  }

  private void closeTempfile(TopicPartition topicPart) throws IOException {
    if (writers.containsKey(topicPart)) {
      RecordWriter writer = writers.get(topicPart);
      writer.close();
      writers.remove(topicPart);
    }
  }

  private void appendToWAL(TopicPartition topicPart) throws IOException {
    long startOffset = offsets.get(topicPart) + 1;
    long endOffset = startOffset + recordCounters.get(topicPart) - 1;
    String tempFileName = tempFileNames.get(topicPart);
    String finalFileName = FileUtils.committedFileName(url, topicsDir, topicPart, startOffset, endOffset);
    WAL wal = wals.get(topicPart);
    wal.append(tempFileName, finalFileName);
  }

  private void commitFile(TopicPartition topicPart) throws IOException {
    long startOffset = offsets.get(topicPart) + 1;
    long endOffset = startOffset + recordCounters.get(topicPart) - 1;
    String tempFileName = tempFileNames.get(topicPart);
    String finalFileName =
        FileUtils.committedFileName(url, topicsDir, topicPart, startOffset, endOffset);
    storage.commit(tempFileName, finalFileName);
    offsets.put(topicPart, endOffset);
    recordCounters.put(topicPart, 0);
  }

  private void setRetryBackoff(long backOffMs) {
    context.timeout(backOffMs);
  }


  public Storage getStorage() {
    return storage;
  }

  public String getTempFileNames(TopicPartition topicPart) {
    return tempFileNames.get(topicPart);
  }

  public RecordWriter getRecordWriter(TopicPartition topicPart) {
    return writers.get(topicPart);
  }

  public WAL getWAL(TopicPartition topicPart) {
    return wals.get(topicPart);
  }

  private void getLastUsedSchema(TopicPartition topicPart) throws IOException {
    if (compatibility == Compatibility.NONE) {
      return;
    }
    // No files from previous execution
    if (!offsets.containsKey(topicPart)) {
      return;
    }
    String path = FileUtils.directoryName(url, topicsDir, topicPart);
    long offset = offsets.get(topicPart);
    PathFilter filter = new CommittedFileWithEndOffsetFilter(offset);
    FileStatus[] statuses = storage.listStatus(path, filter);
    assert statuses.length == 1;

    SeekableInput input = new FsInput(statuses[0].getPath(), conf);
    DatumReader<Object> reader = new GenericDatumReader<>();
    FileReader<Object> fileReader = DataFileReader.openReader(input, reader);
    org.apache.avro.Schema avroSchema = fileReader.getSchema();
    Schema schema = avroData.toConnectSchema(avroSchema);
    schemas.put(topicPart, schema);
  }
}
