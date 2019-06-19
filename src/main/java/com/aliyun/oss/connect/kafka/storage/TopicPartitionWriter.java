/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.oss.connect.kafka.storage;

import com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration;
import io.confluent.common.utils.Time;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import io.confluent.connect.storage.util.DateTimeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class TopicPartitionWriter {
  private static final Logger LOG = LoggerFactory.getLogger(TopicPartitionWriter.class);

  private OSSSinkConnectorConfiguration connectorConfiguration;
  private final Time time;
  private final TopicPartition topicPartition;
  private final SinkTaskContext sinkTaskContext;
  private final RecordWriterProvider<OSSSinkConnectorConfiguration> writerProvider;
  private final Partitioner<FieldSchema> partitioner;
  private final Map<String, RecordWriter> writers;
  private final Map<String, Schema> currentSchemas;
  private final StorageSchemaCompatibility compatibility;
  private final Queue<SinkRecord> buffer;
  private final Map<String, Long> startOffsets;
  private Long offsetToCommit;
  private long currentOffset;
  private final Map<String, String> commitFiles;
  private long retryBackoffMs;
  private long failureTime;

  private final TimestampExtractor timestampExtractor;
  private final long rotateIntervalMs;
  private final long rotateScheduleIntervalMs;
  private long nextScheduledRotation;
  private DateTimeZone timeZone;

  private State state;
  private int writtenRecordCount;

  private String currentEncodedPartition;
  private Long baseRecordTimestamp;
  private Long currentTimestamp;

  private final int flushSize;

  public TopicPartitionWriter(TopicPartition tp,
      RecordWriterProvider<OSSSinkConnectorConfiguration> writerProvider,
      Partitioner<FieldSchema> partitioner,
      OSSSinkConnectorConfiguration connectorConfiguration,
      SinkTaskContext context,
      Time time) {
    this.connectorConfiguration = connectorConfiguration;
    this.time = time;
    this.topicPartition = tp;
    this.sinkTaskContext = context;
    this.writerProvider = writerProvider;
    this.partitioner = partitioner;
    this.writers = new HashMap<>();
    this.buffer = new LinkedList<>();
    this.startOffsets = new HashMap<>();
    this.currentSchemas = new HashMap<>();
    this.currentOffset = -1L;
    this.commitFiles = new HashMap<>();
    this.retryBackoffMs = connectorConfiguration.getLong(
        OSSSinkConnectorConfiguration.RETRY_BACKOFF_CONFIG);
    this.failureTime = -1L;

    this.timestampExtractor = partitioner instanceof TimeBasedPartitioner ?
        ((TimeBasedPartitioner) partitioner).getTimestampExtractor() : null;

    this.rotateIntervalMs = connectorConfiguration.getLong(
        OSSSinkConnectorConfiguration.ROTATE_INTERVAL_MS_CONFIG);

    if (this.rotateIntervalMs > 0 && this.timestampExtractor == null) {
      LOG.warn(
          "Property '{}' is set to '{}ms' but partitioner is not an instance of '{}'. This property"
              + " is ignored.",
          OSSSinkConnectorConfiguration.ROTATE_INTERVAL_MS_CONFIG,
          this.rotateIntervalMs,
          TimeBasedPartitioner.class.getName()
      );
    }

    this.rotateScheduleIntervalMs = connectorConfiguration.getLong(
        OSSSinkConnectorConfiguration.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG);
    if (this.rotateScheduleIntervalMs > 0) {
      this.timeZone = DateTimeZone.forID(
          connectorConfiguration.getString(PartitionerConfig.TIMEZONE_CONFIG));
    }

    this.flushSize = connectorConfiguration.getInt(OSSSinkConnectorConfiguration.FLUSH_SIZE_CONFIG);

    this.compatibility = StorageSchemaCompatibility.getCompatibility(
        connectorConfiguration.getString(HiveConfig.SCHEMA_COMPATIBILITY_CONFIG));

    this.state = State.WRITE_STARTED;
    this.writtenRecordCount = 0;
    setNextScheduleRotation();
  }

  private enum State {
    WRITE_STARTED,
    WRITE_PARTITION_PAUSED,
    SHOULD_ROTATE,
    FILE_COMMITTED;

    private static final State[] VALUES = values();

    public State next() {
      return VALUES[(ordinal() + 1) % VALUES.length];
    }
  }

  public void write() {
    long now = time.milliseconds();
    if (failureTime > 0 && now - failureTime < retryBackoffMs) {
      return;
    }

    while (!buffer.isEmpty()) {
      try {
        executeState(now);
      } catch (SchemaProjectorException | IllegalWorkerStateException e) {
        throw new ConnectException(e);
      } catch (RetriableException e) {
        LOG.error("Exception on topic partition {}: ", topicPartition, e);
        failureTime = time.milliseconds();
        sinkTaskContext.timeout(retryBackoffMs);
        break;
      }
    }

    commitOnTimeIfNoData(now);
  }

  private void executeState(long now) {
    switch (state) {
      case WRITE_STARTED:
        sinkTaskContext.pause(topicPartition);
        state = state.next();
      case WRITE_PARTITION_PAUSED:
        SinkRecord record = buffer.peek();
        if (timestampExtractor != null) {
          currentTimestamp = timestampExtractor.extract(record);
          if (baseRecordTimestamp == null) {
            baseRecordTimestamp = currentTimestamp;
          }
        }

        Schema valueScheme = record.valueSchema();
        String encodedPartition = partitioner.encodePartition(record);
        if (currentSchemas.get(encodedPartition) == null) {
          currentSchemas.put(encodedPartition, valueScheme);
        }

        if (!checkRotationOrAppend(record, currentSchemas.get(encodedPartition), valueScheme, encodedPartition, now)) {
          break;
        }

      case SHOULD_ROTATE:
        commitFiles();
        state = state.next();
      case FILE_COMMITTED:
        state = State.WRITE_PARTITION_PAUSED;
        break;
      default:
        LOG.error("{} is not a valid state to write record for topic partition {}.", state, topicPartition);
    }
  }

  private boolean checkRotationOrAppend(SinkRecord record,
      Schema currentValueSchema, Schema valueSchema,
      String encodedPartition, long now) {
    if (compatibility.shouldChangeSchema(record, null, currentValueSchema) && writtenRecordCount > 0) {
      LOG.trace(
          "Incompatible change of schema detected for record '{}' with encoded partition '{}' and current offset: '{}'",
          record, encodedPartition, currentOffset);
      currentSchemas.put(encodedPartition, valueSchema);
      this.state = state.next();
    } else if (rotateOnTime(encodedPartition, currentTimestamp, now)) {
      setNextScheduleRotation();
      this.state = state.next();
    } else {
      currentEncodedPartition = encodedPartition;
      SinkRecord projectedRecord = compatibility.project(record, null, currentValueSchema);
      writeRecord(projectedRecord);
      buffer.poll();
      if (writtenRecordCount >= flushSize) {
        LOG.info("Starting commit and rotation for topic partition {} with start offset {}",
            topicPartition, startOffsets);
        this.state = state.next();
      } else {
        return false;
      }
    }
    return true;
  }

  private void writeRecord(SinkRecord record) {
    currentOffset = record.kafkaOffset();
    if (!startOffsets.containsKey(currentEncodedPartition)) {
      startOffsets.put(currentEncodedPartition, currentOffset);
    }

    RecordWriter writer;
    if (writers.containsKey(currentEncodedPartition)) {
      writer = writers.get(currentEncodedPartition);
    } else {
      String commitFilename;
      if (commitFiles.containsKey(currentEncodedPartition)) {
        commitFilename = commitFiles.get(currentEncodedPartition);
      } else {
        String prefix = partitioner.generatePartitionedPath(topicPartition.topic(), currentEncodedPartition);
        commitFilename = objectKey(prefix, startOffsets.get(currentEncodedPartition));
        commitFiles.put(currentEncodedPartition, commitFilename);
      }
      writer = writerProvider.getRecordWriter(connectorConfiguration, commitFilename);
      writers.put(currentEncodedPartition, writer);
    }

    writer.write(record);
    ++writtenRecordCount;
  }

  private String objectKey(String dirPrefix, long startOffset) {
    String dirDelim = connectorConfiguration.getString(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    String fileDelim = connectorConfiguration.getString(StorageCommonConfig.FILE_DELIM_CONFIG);
    String extension = writerProvider.getExtension();
    String zeroPadOffsetFormat = "%0"
        + connectorConfiguration.getInt(OSSSinkConnectorConfiguration.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG)
        + "d";
    String name = topicPartition.topic()
        + fileDelim
        + topicPartition.partition()
        + fileDelim
        + String.format(zeroPadOffsetFormat, startOffset)
        + extension;

    String suffix = dirPrefix + dirDelim + name;
    String topicsPrefix = connectorConfiguration.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    return StringUtils.isNotBlank(topicsPrefix) ? topicsPrefix + dirDelim + suffix : suffix;
  }

  private void commitOnTimeIfNoData(long now) {
    if (!buffer.isEmpty()) {
      return;
    }

    // committing files after waiting for rotateIntervalMs time but less than
    // flush.size records available
    if (writtenRecordCount > 0 && rotateOnTime(currentEncodedPartition, currentTimestamp, now)) {
      LOG.info("Committing files after waiting for rotateIntervalMs time but less than flush.size records available.");

      setNextScheduleRotation();

      try {
        commitFiles();
      } catch (ConnectException e) {
        LOG.error("Exception on topic partition {}: ", topicPartition, e);
        failureTime = time.milliseconds();
        sinkTaskContext.timeout(retryBackoffMs);
      }
    }

    LOG.trace("Resuming writer for topic partition '{}'", topicPartition);
    sinkTaskContext.resume(topicPartition);
    this.state = State.WRITE_STARTED;
  }

  private void commitFiles() {
    for (Map.Entry<String, String> entry : commitFiles.entrySet()) {
      String encodedPartition = entry.getKey();
      if (!startOffsets.containsKey(encodedPartition)) {
        LOG.warn("Tried to commit file with missing starting offset partition: {}. Ignoring", encodedPartition);
        continue;
      }

      if (writers.containsKey(encodedPartition)) {
        RecordWriter writer = writers.get(encodedPartition);
        writer.commit();
        writers.remove(encodedPartition);
      }

      startOffsets.remove(encodedPartition);
    }

    offsetToCommit = currentOffset + 1;
    commitFiles.clear();
    currentSchemas.clear();
    writtenRecordCount = 0;
    baseRecordTimestamp = null;
    LOG.info("Files committed to OSS. Target commit offset for {} is {}", topicPartition, offsetToCommit);
  }

  private boolean rotateOnTime(String encodedPartition, Long recordTimestamp, long now) {
    if (writtenRecordCount <= 0) {
      return false;
    }

    boolean periodicRotation = rotateIntervalMs > 0
        && timestampExtractor != null
        && (recordTimestamp - baseRecordTimestamp >= rotateIntervalMs
            || !encodedPartition.equals(currentEncodedPartition));

    LOG.trace(
        "Should apply periodic time-based rotation (rotateIntervalMs: '{}', baseRecordTimestamp: "
            + "'{}', timestamp: '{}', encodedPartition: '{}', currentEncodedPartition: '{}')? {}",
        rotateIntervalMs,
        baseRecordTimestamp,
        recordTimestamp,
        encodedPartition,
        currentEncodedPartition,
        periodicRotation
    );

    boolean scheduledRotation = rotateScheduleIntervalMs > 0 && now >= nextScheduledRotation;
    LOG.trace(
        "Should apply scheduled rotation: (rotateScheduleIntervalMs: '{}', nextScheduledRotation:"
            + " '{}', now: '{}')? {}",
        rotateScheduleIntervalMs,
        nextScheduledRotation,
        now,
        scheduledRotation
    );
    return periodicRotation || scheduledRotation;
  }

  private void setNextScheduleRotation() {
    if (rotateScheduleIntervalMs > 0) {
      nextScheduledRotation = DateTimeUtils.getNextTimeAdjustedByDay(
          time.milliseconds(), rotateScheduleIntervalMs, timeZone);

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Update scheduled rotation timer. Next rotation for {} will be at {}",
            topicPartition,
            new DateTime(nextScheduledRotation).withZone(timeZone).toString()
        );
      }
    }
  }

  public void close() throws ConnectException {
    LOG.debug("Closing TopicPartitionWriter {}", topicPartition);
    for (RecordWriter writer : writers.values()) {
      writer.close();
    }

    writers.clear();
    startOffsets.clear();
  }

  public Long getOffsetToCommitAndReset() {
    Long latest = offsetToCommit;
    offsetToCommit = null;
    return latest;
  }

  public void buffer(SinkRecord record) {
    buffer.add(record);
  }
}
