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

package com.aliyun.oss.connect.kafka;

import com.aliyun.oss.connect.kafka.storage.OSSStorage;
import com.aliyun.oss.connect.kafka.storage.TopicPartitionWriter;
import com.aliyun.oss.connect.kafka.utils.Version;
import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.storage.Storage;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OSSSinkTask extends SinkTask {
  private static final Logger LOG = LoggerFactory.getLogger(OSSSinkTask.class);

  private OSSSinkConnectorConfiguration connectorConfig;
  private String url;
  private Storage storage;
  private final Set<TopicPartition> assignment;
  private final Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;
  private RecordWriterProvider<OSSSinkConnectorConfiguration> writerProvider;
  private Partitioner<FieldSchema> partitioner;
  private final Time time;
  private Format<OSSSinkConnectorConfiguration, String> format;

  public OSSSinkTask() {
    assignment = new HashSet<>();
    topicPartitionWriters = new HashMap<>();
    time = new SystemTime();
  }

  OSSSinkTask(OSSSinkConnectorConfiguration connectorConfig,
      SinkTaskContext context, OSSStorage storage,
      Partitioner<FieldSchema> partitioner,
      Format<OSSSinkConnectorConfiguration, String> format, Time time) {
    this.assignment = new HashSet<>();
    this.topicPartitionWriters = new HashMap<>();
    this.connectorConfig = connectorConfig;
    this.storage = storage;
    this.partitioner = partitioner;
    this.context = context;
    this.format = format;
    this.time = time;
    url = connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG);
    writerProvider = this.format.getRecordWriterProvider();

    open(context.assignment());
    LOG.info("Started OSS connector task with assigned partitions {}", assignment);
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    try {
      connectorConfig = new OSSSinkConnectorConfiguration(properties);

      url = connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG);

      Class<? extends OSSStorage> storageClass =
          (Class<? extends OSSStorage>)
              connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);

      storage = StorageFactory.createStorage(storageClass, OSSSinkConnectorConfiguration.class, connectorConfig, url);
      writerProvider = createFormat().getRecordWriterProvider();
      partitioner = createPartitioner();

      open(context.assignment());

      LOG.info("Started OSS connector task with assigned partitions: {}", assignment);
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  private Format<OSSSinkConnectorConfiguration, String> createFormat() throws Exception {
    Class<Format<OSSSinkConnectorConfiguration, String>> formatClass =
        (Class<Format<OSSSinkConnectorConfiguration, String>>) connectorConfig.getClass(
            OSSSinkConnectorConfiguration.FORMAT_CLASS_CONFIG);

    return formatClass.getConstructor(OSSStorage.class).newInstance(storage);
  }

  private Partitioner<FieldSchema> createPartitioner() throws Exception {
    Class<? extends Partitioner<FieldSchema>> partitionClass =
        (Class<? extends Partitioner<FieldSchema>>) connectorConfig.getClass(
            PartitionerConfig.PARTITIONER_CLASS_CONFIG);

    Partitioner<FieldSchema> partitioner = partitionClass.newInstance();
    Map<String, Object> plainValues = new HashMap<>(connectorConfig.plainValues());
    Map<String, ?> originals = connectorConfig.originals();
    for (String originalKey : originals.keySet()) {
      if (!plainValues.containsKey(originalKey)) {
        plainValues.put(originalKey, originals.get(originalKey));
      }
    }

    partitioner.configure(plainValues);
    return partitioner;
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    assignment.addAll(partitions);
    for (TopicPartition topicPartition : partitions) {
      TopicPartitionWriter writer = new TopicPartitionWriter(
          topicPartition,
          writerProvider,
          partitioner,
          connectorConfig,
          context,
          time
      );

      topicPartitionWriters.put(topicPartition, writer);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      String topic = record.topic();
      int partition = record.kafkaPartition();
      TopicPartition tp = new TopicPartition(topic, partition);
      topicPartitionWriters.get(tp).buffer(record);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Read {} records from Kafka", records.size());
    }

    for (TopicPartition tp : assignment) {
      topicPartitionWriters.get(tp).write();
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // No-op. The connector is managing the offsets.
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : assignment) {
      try {
        topicPartitionWriters.get(tp).close();
      } catch (ConnectException e) {
        LOG.error("Error closing writer for {}. Error: {}", tp, e.getMessage());
      }
    }

    topicPartitionWriters.clear();
    assignment.clear();
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> offsets) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    for (TopicPartition topicPartition : assignment) {
      Long offset = topicPartitionWriters.get(topicPartition).getOffsetToCommitAndReset();
      if (offset != null) {
        LOG.trace("Forwarding to framework request to commit offset: {} for {}", offset, topicPartition);
        offsetsToCommit.put(topicPartition, new OffsetAndMetadata(offset));
      }
    }
    return offsetsToCommit;
  }

  @Override
  public void stop() {
    try {
      if (storage != null) {
        storage.close();
      }
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  // Visible for testing
  TopicPartitionWriter getTopicPartitionWriter(TopicPartition tp) {
    return topicPartitionWriters.get(tp);
  }

  // Visible for testing
  Format<OSSSinkConnectorConfiguration, String> getFormat() {
    return format;
  }
}
