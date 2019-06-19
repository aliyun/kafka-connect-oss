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

import com.aliyun.oss.connect.kafka.format.avro.AvroFormat;
import com.aliyun.oss.connect.kafka.storage.OSSStorage;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

import static io.confluent.connect.avro.AvroData.AVRO_TYPE_ENUM;
import static io.confluent.connect.avro.AvroData.CONNECT_ENUM_DOC_PROP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestDataWriterAvro extends TestOSSSinkConnectorBase {

  private static final String ZERO_PAD_FMT = "%010d";

  protected OSSStorage storage;
  private AvroFormat format;
  Partitioner<FieldSchema> partitioner;
  OSSSinkTask task;
  Map<String, String> localProps = new HashMap<>();

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  public void setUp() throws Exception {
    super.setUp();

    storage = new OSSStorage(connectorConfig, url);
    partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);

    format = new AvroFormat(storage);
  }

  public String getExtension() {
    return ".avro";
  }

  public AvroData getAvroData() {
    return format.getAvroData();
  }

  public Format<OSSSinkConnectorConfiguration, String> getFormat() {
    return format;
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    localProps.clear();
    storage.delete(connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG));
  }

  @Test
  public void testWriteRecords() throws Exception {
    setUp();
    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecords(TEST_RECORDS);
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    verify(sinkRecords, VALID_OFFSETS);
  }

  @Test
  public void testWriteRecordsInMultiplePartitions() throws Exception {
    setUp();
    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecords(
        TEST_RECORDS, 0, context.assignment());
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    verify(sinkRecords, VALID_OFFSETS, context.assignment());
  }

  @Test
  public void testWriteInterleavedRecordsInMultiplePartitions() throws Exception {
    setUp();
    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecordsInterleaved(
        TEST_RECORDS * context.assignment().size(), 0, context.assignment());
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    verify(sinkRecords, VALID_OFFSETS, context.assignment());
  }

  @Test
  public void testSnappyCodec() throws Exception {
    localProps.put(StorageSinkConnectorConfig.AVRO_CODEC_CONFIG, "snappy");
    setUp();
    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecords(TEST_RECORDS);
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    verify(sinkRecords, VALID_OFFSETS);
  }

  @Test
  public void testWriteInterleavedRecordsInMultiplePartitionsWithNonZeroInitialOffset() throws Exception {
    setUp();
    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    long start = TEST_FLUSH_SIZE + 12345;
    List<SinkRecord> sinkRecords = createRecordsInterleaved(
        TEST_RECORDS * context.assignment().size(),
        start, context.assignment());
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = new long[VALID_OFFSETS.length];
    for (int i = 0; i < validOffsets.length; ++i) {
      validOffsets[i] = start + VALID_OFFSETS[i];
    }
    verify(sinkRecords, validOffsets, context.assignment());
  }

  @Test
  public void testWriteRecordsOfEnumsWithEnhancedAvroData() throws Exception {
    localProps.put(StorageSinkConnectorConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, "true");
    localProps.put(StorageSinkConnectorConfig.CONNECT_META_DATA_CONFIG, "true");
    setUp();

    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecordsWithEnums(
        TEST_RECORDS, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    verify(sinkRecords, VALID_OFFSETS);
  }

  @Test
  public void testWriteRecordsOfUnionsWithEnhancedAvroData() throws Exception {
    localProps.put(StorageSinkConnectorConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, "true");
    localProps.put(StorageSinkConnectorConfig.CONNECT_META_DATA_CONFIG, "true");
    setUp();

    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecordsWithUnion(
        TEST_RECORDS, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    verify(sinkRecords, VALID_OFFSETS);
  }

  @Test
  public void testRecovery() throws Exception {
    setUp();

    List<SinkRecord> sinkRecords = createRecords(1000);
    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    sinkRecords.addAll(createRecords(TEST_RECORDS - 1000, 1000));
    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    verify(sinkRecords, VALID_OFFSETS);
  }

  @Test
  public void testPreCommitOnSizeRotation() throws Exception {
    setUp();
    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecordsInterleaved(
        TEST_RECORDS * context.assignment().size(), 0, context.assignment());

    task.put(sinkRecords);

    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = task.preCommit(null);

    long offset = TEST_RECORDS / TEST_FLUSH_SIZE * TEST_FLUSH_SIZE;
    verifyOffsets(offsetsToCommit, new long[]{offset, offset},
        context.assignment().toArray(new TopicPartition[0]));

    sinkRecords = createRecordsInterleaved(
        TEST_RECORDS * context.assignment().size(), TEST_RECORDS, context.assignment());
    task.put(sinkRecords);
    offsetsToCommit = task.preCommit(null);

    offset = 2 * TEST_RECORDS / TEST_FLUSH_SIZE * TEST_FLUSH_SIZE;
    verifyOffsets(offsetsToCommit, new long[]{offset, offset},
        context.assignment().toArray(new TopicPartition[0]));

    sinkRecords = createRecordsInterleaved(
        2 * context.assignment().size(), 2 * TEST_RECORDS, context.assignment());

    task.put(sinkRecords);
    offsetsToCommit = task.preCommit(null);

    verifyOffsets(offsetsToCommit, new long[]{-1, -1},
        context.assignment().toArray(new TopicPartition[0]));

    task.close(context.assignment());
    task.stop();
  }

  @Test
  public void testPreCommitOnSchemaIncompatibilityRotation() throws Exception {
    localProps.put(OSSSinkConnectorConfiguration.FLUSH_SIZE_CONFIG, TEST_FLUSH_SIZE * 2 + "");
    setUp();

    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecordsWithAlteringSchemas(TEST_FLUSH_SIZE * 2, 0);

    task.put(sinkRecords);
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = task.preCommit(null);

    verifyOffsets(offsetsToCommit, new long[]{TEST_FLUSH_SIZE, -1},
        context.assignment().toArray(new TopicPartition[0]));

    task.close(context.assignment());
    task.stop();
  }

  @Test
  public void testPartitionsRebalanced() throws Exception {
    setUp();

    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecordsInterleaved(
        TEST_RECORDS * context.assignment().size(), 0, context.assignment());
    Set<TopicPartition> originalAssignment = new HashSet<>(context.assignment());
    Set<TopicPartition> nextAssignment = new HashSet<>();
    nextAssignment.add(TOPIC_PARTITION);
    nextAssignment.add(TOPIC_PARTITION3);

    task.put(sinkRecords);
    task.close(context.assignment());

    context.setAssignment(nextAssignment);
    task.open(context.assignment());

    assertNull(task.getTopicPartitionWriter(TOPIC_PARTITION2));
    assertNotNull(task.getTopicPartitionWriter(TOPIC_PARTITION));
    assertNotNull(task.getTopicPartitionWriter(TOPIC_PARTITION3));

    verify(sinkRecords, VALID_OFFSETS, originalAssignment);

    sinkRecords = createRecordsInterleaved(
        TEST_RECORDS * context.assignment().size(),
        VALID_OFFSETS[VALID_OFFSETS.length - 1], context.assignment());
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = new long[2 * TEST_RECORDS / TEST_FLUSH_SIZE];
    for (int i = 0; i < validOffsets.length; ++i) {
      validOffsets[i] = TEST_FLUSH_SIZE * i;
    }
    verify(sinkRecords, validOffsets, Collections.singleton(TOPIC_PARTITION));
    verify(sinkRecords, VALID_OFFSETS, Collections.singleton(TOPIC_PARTITION2));
    validOffsets = new long[validOffsets.length - VALID_OFFSETS.length + 1];
    for (int i = 0; i < validOffsets.length; ++i) {
      validOffsets[i] = TEST_FLUSH_SIZE * (i + VALID_OFFSETS.length - 1);
    }
    verify(sinkRecords, validOffsets, Collections.singleton(TOPIC_PARTITION3));
  }

  @Test
  public void testSchemaCompatibilityBackward() throws Exception {
    localProps.put(HiveConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
    setUp();

    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecordsWithAlteringSchemas(TEST_RECORDS, 0);

    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    verify(sinkRecords, VALID_OFFSETS);
  }

  @Test
  public void testSchemaCompatibilityNone() throws Exception {
    localProps.put(HiveConfig.SCHEMA_COMPATIBILITY_CONFIG, "NONE");
    setUp();

    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecordsWithAlteringSchemas(TEST_RECORDS, 0);

    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    verify(sinkRecords, VALID_OFFSETS);
  }

  @Test
  public void testSchemaCompatibilityForward() throws Exception {
    localProps.put(HiveConfig.SCHEMA_COMPATIBILITY_CONFIG, "FORWARD");
    setUp();

    task = new OSSSinkTask(connectorConfig, context, storage,
        partitioner, getFormat(), SYSTEM_TIME);

    int size = TEST_RECORDS / TEST_FLUSH_SIZE * TEST_FLUSH_SIZE * 2;
    List<SinkRecord> sinkRecords = createRecordsWithAlteringSchemas(size, 0)
        .subList(TEST_FLUSH_SIZE, size);

    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = new long[size / TEST_FLUSH_SIZE - 1];
    for (int i = 0; i < validOffsets.length; i++) {
      validOffsets[i] = (i + 1) * TEST_FLUSH_SIZE;
    }
    verify(sinkRecords, validOffsets);
  }

  protected List<SinkRecord> createRecordsWithAlteringSchemas(int size, long startOffset) {
    Schema schema = createSchema();
    Struct record = createRecord(schema);
    Schema newSchema = createNewSchema();
    Struct newRecord = createNewRecord(newSchema);

    int limit = (size / TEST_FLUSH_SIZE) * TEST_FLUSH_SIZE;
    boolean remainder = size % TEST_FLUSH_SIZE > 0;
    List<SinkRecord> sinkRecords = new ArrayList<>();
    String key = "test";
    for (long offset = startOffset; offset < startOffset + limit;) {
      for (int i = 0; i < TEST_FLUSH_SIZE; ++i) {
        sinkRecords.add(new SinkRecord(TOPIC, PARTITION,
            Schema.STRING_SCHEMA, key, schema, record, offset++));
      }

      for (int i = 0; i < TEST_FLUSH_SIZE; ++i) {
        sinkRecords.add(new SinkRecord(TOPIC, PARTITION,
            Schema.STRING_SCHEMA, key, newSchema, newRecord, offset++));
      }
    }
    if (remainder) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION,
          Schema.STRING_SCHEMA, key, schema, record, startOffset + size - 1));
    }
    return sinkRecords;
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets) throws IOException {
    verify(sinkRecords, validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
  }

  protected void verify(List<SinkRecord> sinkRecords,
      long[] validOffsets, Set<TopicPartition> partitions) throws IOException {
    for (TopicPartition tp : partitions) {
      for (int i = 1, j = 0; i < validOffsets.length; ++i) {
        long startOffset = validOffsets[i - 1];
        long size = validOffsets[i] - startOffset;

        Collection<Object> records = readRecords(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset,
            getExtension(), ZERO_PAD_FMT, OSS_TEST_BUCKET);
        assertEquals(size, records.size());
        verifyContents(sinkRecords, j, records);
        j += size;
      }
    }
  }

  protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> records) {
    Schema expectedSchema = null;
    for (Object avroRecord : records) {
      if (expectedSchema == null) {
        expectedSchema = expectedRecords.get(startIndex).valueSchema();
      }
      Object expectedValue = SchemaProjector.project(expectedRecords.get(startIndex).valueSchema(),
          expectedRecords.get(startIndex++).value(), expectedSchema);
      Object value = getAvroData().fromConnectData(expectedSchema, expectedValue);
      // AvroData wraps primitive types so their schema can be included. We need to unwrap
      // NonRecordContainers to just their value to properly handle these types
      if (value instanceof NonRecordContainer) {
        value = ((NonRecordContainer) value).getValue();
      }
      if (avroRecord instanceof Utf8) {
        assertEquals(value, avroRecord.toString());
      } else {
        assertEquals(value, avroRecord);
      }
    }
  }

  protected void verifyOffsets(Map<TopicPartition, OffsetAndMetadata> actualOffsets,
      long[] validOffsets, TopicPartition[] partitions) {
    Map<TopicPartition, OffsetAndMetadata> expectedOffsets = new HashMap<>();
    for (int i = 0; i < validOffsets.length; ++i) {
      long offset = validOffsets[i];
      if (offset >= 0) {
        expectedOffsets.put(partitions[i], new OffsetAndMetadata(offset, ""));
      }
    }
    assertTrue(Objects.equals(actualOffsets, expectedOffsets));
  }

  protected String getDirectory(String topic, int partition) {
    String encodedPartition = "partition=" + partition;
    return partitioner.generatePartitionedPath(topic, encodedPartition);
  }

  protected List<SinkRecord> createRecords(int size) {
    return createRecords(size, 0);
  }

  protected List<SinkRecord> createRecords(int size, long startOffset) {
    return createRecords(size, startOffset, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
  }

  protected List<SinkRecord> createRecords(int size, long startOffset, Set<TopicPartition> partitions) {
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition topicPartition : partitions) {
      for (long offset = startOffset; offset < startOffset + size; ++offset) {
        sinkRecords.add(new SinkRecord(TOPIC, topicPartition.partition(),
            Schema.STRING_SCHEMA, "key", schema, record, offset));
      }
    }
    return sinkRecords;
  }

  protected List<SinkRecord> createRecordsInterleaved(
      int size, long startOffset, Set<TopicPartition> partitions) {
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = startOffset, total = 0; total < size; ++offset) {
      for (TopicPartition topicPartition : partitions) {
        sinkRecords.add(new SinkRecord(TOPIC, topicPartition.partition(),
            Schema.STRING_SCHEMA, "key", schema, record, offset));
        if (++total >= size) {
          break;
        }
      }
    }
    return sinkRecords;
  }

  private List<SinkRecord> createRecordsWithEnums(
      int size, long startOffset, Set<TopicPartition> partitions) {
    SchemaBuilder builder = SchemaBuilder.string().name("TestOSSEnum");
    builder.parameter(CONNECT_ENUM_DOC_PROP, "");
    builder.parameter(AVRO_TYPE_ENUM, "TestOSSEnum");
    for (String enumSymbol : new String[]{"hello", "oss", "world"}) {
      builder.parameter(AVRO_TYPE_ENUM + "." + enumSymbol, enumSymbol);
    }
    Schema schema = builder.build();
    SchemaAndValue valueAndSchema = new SchemaAndValue(schema, "oss");
    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition topicPartition : partitions) {
      for (long offset = startOffset; offset < startOffset + size; ++offset) {
        sinkRecords.add(new SinkRecord(TOPIC, topicPartition.partition(),
            Schema.STRING_SCHEMA, "key", schema, valueAndSchema.value(), offset));
      }
    }
    return sinkRecords;
  }

  private List<SinkRecord> createRecordsWithUnion(
      int size, long startOffset, Set<TopicPartition> partitions) {
    Schema subRecordOneSchema = SchemaBuilder.struct()
        .name("SubRecordOne")
        .field("string", Schema.STRING_SCHEMA).optional().build();

    Schema subRecordTwoSchema = SchemaBuilder.struct()
        .name("SubRecordTwo")
        .field("string", Schema.STRING_SCHEMA).optional().build();

    Schema schema = SchemaBuilder.struct()
        .name("TestUnion")
        .field("int", Schema.OPTIONAL_INT32_SCHEMA)
        .field("string", Schema.OPTIONAL_STRING_SCHEMA)
        .field("SubRecordOne", subRecordOneSchema)
        .field("SubRecordTwo", subRecordTwoSchema)
        .build();

    SchemaAndValue intValue = new SchemaAndValue(
        schema, new Struct(schema).put("int", 100));
    SchemaAndValue strValue = new SchemaAndValue(
        schema, new Struct(schema).put("string", "oss"));

    SchemaAndValue subRecordOne = new SchemaAndValue(
        schema, new Struct(schema).put("SubRecordOne",
            new Struct(subRecordOneSchema).put("string", "hello")));
    SchemaAndValue subRecordTwo = new SchemaAndValue(
        schema, new Struct(schema).put("SubRecordTwo",
            new Struct(subRecordTwoSchema).put("string", "world")));

    String key = "test";
    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition topicPartition : partitions) {
      for (long offset = startOffset; offset < startOffset + 4 * size;) {
        sinkRecords.add(new SinkRecord(TOPIC, topicPartition.partition(),
            Schema.STRING_SCHEMA, key, schema, intValue.value(), offset++));
        sinkRecords.add(new SinkRecord(TOPIC, topicPartition.partition(),
            Schema.STRING_SCHEMA, key, schema, strValue.value(), offset++));
        sinkRecords.add(new SinkRecord(TOPIC, topicPartition.partition(),
            Schema.STRING_SCHEMA, key, schema, subRecordOne.value(), offset++));
        sinkRecords.add(new SinkRecord(TOPIC, topicPartition.partition(),
            Schema.STRING_SCHEMA, key, schema, subRecordTwo.value(), offset++));
      }
    }

    return sinkRecords;
  }
}
