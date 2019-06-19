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

import com.aliyun.oss.connect.kafka.format.parquet.ParquetJsonFormat;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.confluent.connect.storage.format.Format;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TestDataWriterParquetJson extends TestDataWriterJson {
  private ParquetJsonFormat format;
  private static final String TEST_PROTO_CLASS = "com.aliyun.oss.connect.kafka.format.parquet.Parquet$TestMessage";

  public String getExtension() {
    return ".parquet";
  }

  public Format<OSSSinkConnectorConfiguration, String> getFormat() {
    return format;
  }

  public void setUp() throws Exception {
    super.setUp();
    format = new ParquetJsonFormat(storage);
  }

  @Test
  public void testGzipWithoutSchema() throws Exception {
    setUp();
    // Ignored
  }

  @Test
  public void testGzipWithSchema() throws Exception {
    setUp();
    // Ignored
  }

  @Test
  public void testWithSchema() throws Exception {
    localProps.put("oss.parquet.protobuf.schema.class", "test-topic, " + TEST_PROTO_CLASS);
    setUp();
    super.testWithSchema();
  }

  @Test
  public void testWithoutSchema() throws Exception {
    localProps.put("oss.parquet.protobuf.schema.class", "test-topic, " + TEST_PROTO_CLASS);
    setUp();
    super.testWithoutSchema();
  }

  @Override
  protected Schema createSchema() {
    return SchemaBuilder.struct().name("record").version(1)
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("int", Schema.INT32_SCHEMA)
        .field("long", Schema.OPTIONAL_INT64_SCHEMA)
        .field("float", Schema.FLOAT32_SCHEMA)
        .field("double", Schema.FLOAT64_SCHEMA)
        .field("map1", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
        .field("map2", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
        .field("list1", SchemaBuilder.array(Schema.STRING_SCHEMA))
        .field("list2", SchemaBuilder.array(Schema.STRING_SCHEMA))
        .build();
  }

  @Override
  protected Struct createRecord(Schema schema) {
    Map<String, String> mapField = new HashMap<>();
    mapField.put("a1", "b1");
    mapField.put("a2", "b2");
    mapField.put("a3", "b3");

    List<String> listField = new ArrayList<>();
    listField.add("a");
    listField.add("b");

    return new Struct(schema)
        .put("boolean", true)
        .put("int", 12)
        .put("long", null)
        .put("float", 12.2f)
        .put("double", 12.2)
        .put("map1", mapField)
        .put("map2", mapField)
        .put("list1", listField)
        .put("list2", listField);
  }

  private Message.Builder getBuilder(String type) {
    try {
      Class<? extends Message> typeClass = (Class<? extends Message>) Class.forName(type);
      Method getBuilder = typeClass.getDeclaredMethod("newBuilder");
      return (Message.Builder) getBuilder.invoke(typeClass);
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  @Override
  protected List<SinkRecord> createJsonRecordsWithoutSchema(
      int size, long startOffset, Set<TopicPartition> partitions) {
    List<SinkRecord> records = new ArrayList<>();
    for (long offset = startOffset, total = 0; total < size; ++offset) {
      for (TopicPartition topicPartition : partitions) {
        String record = "{\"boolean\": true,"
            + "\"int\": " + 100 + ","
            + "\"long\": " + null + ","
            + "\"float\": " + 100.0 + ","
            + "\"double\": " + 100.0 + ","
            + "\"map1\": " + "{\"a1\":\"b1\", \"a2\":\"b2\", \"a3\":\"b3\"}" + ","
            + "\"map2\": " + null + ","
            + "\"list1\": " + null + ","
            + "\"list2\": " + "[\"a\", \"b\"]"
            + "}";

        records.add(new SinkRecord(TOPIC, topicPartition.partition(),
            null, "key", null, record, offset));
        if (++total >= size) {
          break;
        }
      }
    }
    return records;
  }

  @Override
  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets,
      Set<TopicPartition> partitions, String extension) throws IOException {
    verify(sinkRecords, validOffsets, partitions, extension, true);
  }

  protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex,
      Collection<Object> records) throws IOException {
    JsonFormat.Parser parser = JsonFormat.parser();
    Message.Builder builder = getBuilder(TEST_PROTO_CLASS);
    for (Object jsonRecord : records) {
      SinkRecord expectedRecord = expectedRecords.get(startIndex++);
      Object expectedValue = expectedRecord.value();
      if (expectedValue instanceof Struct) {
        byte[] expectedBytes = converter.fromConnectData(TOPIC,
            expectedRecord.valueSchema(), expectedRecord.value());
        parser.merge(new String(expectedBytes), builder);
      } else {
        parser.merge(expectedValue.toString(), builder);
      }

      expectedValue = builder.build();
      Message message = ((Message.Builder) jsonRecord).build();
      assertEquals(expectedValue, message.toBuilder().build());
      builder.clear();
    }
  }
}
