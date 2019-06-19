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

import com.aliyun.oss.connect.kafka.format.json.JsonFormat;
import com.aliyun.oss.connect.kafka.storage.CompressionType;
import com.aliyun.oss.connect.kafka.storage.OSSStorage;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TestDataWriterJson extends TestOSSSinkConnectorBase {
  private static final String ZERO_PAD_FMT = "%010d";

  protected final ObjectMapper mapper = new ObjectMapper();
  protected JsonConverter converter;
  protected OSSStorage storage;
  private JsonFormat format;
  Partitioner<FieldSchema> partitioner;
  OSSSinkTask task;
  protected Map<String, String> localProps = new HashMap<>();

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  public void setUp() throws Exception {
    super.setUp();
    converter = new JsonConverter();

    converter.configure(
        Collections.singletonMap("schemas.enable", "false"), false);

    storage = new OSSStorage(connectorConfig, url);
    partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);

    format = new JsonFormat(storage);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    localProps.clear();
    storage.delete(
        connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG));
  }

  public Format<OSSSinkConnectorConfiguration, String> getFormat() {
    return format;
  }

  public String getExtension() {
    return ".json";
  }

  @Test
  public void testWithoutSchema() throws Exception {
    setUp();

    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> records = createJsonRecordsWithoutSchema(
        TEST_RECORDS * context.assignment().size(), 0, context.assignment());
    task.put(records);
    task.close(context.assignment());
    task.stop();

    verify(records, VALID_OFFSETS, context.assignment(), getExtension());
  }

  @Test
  public void testGzipWithoutSchema() throws Exception {
    localProps.put(OSSSinkConnectorConfiguration.COMPRESSION_TYPE_CONFIG,
        CompressionType.GZIP.name);
    setUp();

    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> records = createJsonRecordsWithoutSchema(
        TEST_RECORDS * context.assignment().size(), 0, context.assignment());
    task.put(records);
    task.close(context.assignment());
    task.stop();

    verify(records, VALID_OFFSETS, context.assignment(), ".json.gz");
  }

  @Test
  public void testWithSchema() throws Exception {
    setUp();

    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> records = createJsonRecordsWithSchema(
        TEST_RECORDS * context.assignment().size(), 0, context.assignment());
    task.put(records);
    task.close(context.assignment());
    task.stop();

    verify(records, VALID_OFFSETS, context.assignment(), getExtension());
  }

  @Test
  public void testGzipWithSchema() throws Exception {
    localProps.put(OSSSinkConnectorConfiguration.FORMAT_CLASS_CONFIG,
        JsonFormat.class.getName());
    localProps.put(OSSSinkConnectorConfiguration.COMPRESSION_TYPE_CONFIG,
        CompressionType.GZIP.name);
    setUp();

    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> records = createJsonRecordsWithSchema(
        TEST_RECORDS * context.assignment().size(), 0, context.assignment());
    task.put(records);
    task.close(context.assignment());
    task.stop();

    verify(records, VALID_OFFSETS, context.assignment(), ".json.gz");
  }

  private List<SinkRecord> createJsonRecordsWithSchema(
      int size, long startOffset, Set<TopicPartition> partitions) {
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    List<SinkRecord> records = new ArrayList<>();
    for (long offset = startOffset, total = 0; total < size; ++offset) {
      for (TopicPartition topicPartition : partitions) {
        records.add(new SinkRecord(TOPIC, topicPartition.partition(),
            Schema.STRING_SCHEMA, "key", schema, record, offset));
        if (++total >= size) {
          break;
        }
      }
    }
    return records;
  }

  protected List<SinkRecord> createJsonRecordsWithoutSchema(
      int size, long startOffset, Set<TopicPartition> partitions) {
    List<SinkRecord> records = new ArrayList<>();
    for (long offset = startOffset, total = 0; total < size; ++offset) {
      for (TopicPartition topicPartition : partitions) {
        String record = "{\"schema\":{\"type\":\"struct\",\"fields\":[ "
            + "{\"type\":\"boolean\",\"optional\":true,\"field\":\"booleanField\"},"
            + "{\"type\":\"int32\",\"optional\":true,\"field\":\"intField\"},"
            + "{\"type\":\"int64\",\"optional\":true,\"field\":\"longField\"},"
            + "{\"type\":\"string\",\"optional\":false,\"field\":\"stringField\"}],"
            + "\"payload\":"
            + "{\"booleanField\":\"true\","
            + "\"intField\":" + 100 + ","
            + "\"longField\":" + 100 + ","
            + "\"stringField\":str" + 100 + "}}";

        records.add(new SinkRecord(TOPIC, topicPartition.partition(),
            null, "key", null, record, offset));
        if (++total >= size) {
          break;
        }
      }
    }
    return records;
  }

  protected String getDirectory(TopicPartition tp) {
    String encodedPartition = "partition=" + tp.partition();
    return partitioner.generatePartitionedPath(tp.topic(), encodedPartition);
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets,
      Set<TopicPartition> partitions, String extension) throws IOException {
    verify(sinkRecords, validOffsets, partitions, extension, false);
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets,
      Set<TopicPartition> partitions, String extension, boolean isProto) throws IOException {

    for (TopicPartition topicPartition : partitions) {
      for (int i = 1, j = 0; i < validOffsets.length; ++i) {
        long startOffset = validOffsets[i - 1];
        long size = validOffsets[i] - startOffset;

        Collection<Object> records = readRecords(topicsDir,
            getDirectory(topicPartition), topicPartition, startOffset,
            extension, ZERO_PAD_FMT, OSS_TEST_BUCKET, isProto);
        verifyContents(sinkRecords, j, records);
        j += size;
      }
    }
  }

  protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex,
      Collection<Object> records) throws IOException{
    for (Object jsonRecord : records) {
      SinkRecord expectedRecord = expectedRecords.get(startIndex++);
      Object expectedValue = expectedRecord.value();
      if (expectedValue instanceof Struct) {
        byte[] expectedBytes = converter.fromConnectData(TOPIC,
            expectedRecord.valueSchema(), expectedRecord.value());
        expectedValue = mapper.readValue(expectedBytes, Object.class);
      }
      assertEquals(expectedValue, jsonRecord);
    }
  }
}
