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

package com.aliyun.oss.connect.kafka.format.parquet;

import com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration;
import com.aliyun.oss.connect.kafka.storage.OSSStorage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

import static com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration.OSS_BUCKET;
import static com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration.PARQUET_BLOCK_SIZE;
import static com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration.PARQUET_COMPRESSION_CODEC;
import static com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration.PARQUET_PAGE_SIZE;
import static com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration.PARQUET_PROTOBUF_SCHEMA_CLASS;

public class ParquetJsonRecordWriterProvider implements RecordWriterProvider<OSSSinkConnectorConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetJsonRecordWriterProvider.class);

  private static final String EXTENSION = ".parquet";
  private final OSSStorage storage;
  private final JsonConverter converter;
  private JsonFormat.Parser parser = JsonFormat.parser();

  ParquetJsonRecordWriterProvider(OSSStorage storage, JsonConverter converter) {
    this.storage = storage;
    this.converter = converter;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final OSSSinkConnectorConfiguration conf, final String filename) {
    return new RecordWriter() {

      ParquetWriter<Message> writer = null;
      Message.Builder builder = null;
      final String name = "oss://"
          + conf.get(OSS_BUCKET)
          + conf.getString(StorageCommonConfig.DIRECTORY_DELIM_CONFIG)
          + filename;

      @Override
      public void write(SinkRecord sinkRecord) {
        try {
          if (writer == null) {
            storage.delete(filename);

            List<String> topicSchemas = conf.getList(PARQUET_PROTOBUF_SCHEMA_CLASS);
            String topic = sinkRecord.topic();
            String schemaClass = null;
            for (int i = 0; i < topicSchemas.size(); ++i) {
              if (topicSchemas.get(i).equals(topic)) {
                schemaClass = topicSchemas.get(i + 1);
              }
            }

            if (schemaClass == null) {
              throw new ConnectException("Can not find protobuf schema class for topic " + topic);
            }

            Class<? extends Message> typeClass = (Class<? extends Message>) Class.forName(schemaClass);
            writer = new ProtoParquetWriter<>(
                new Path(name),
                typeClass,
                CompressionCodecName.valueOf(conf.getString(PARQUET_COMPRESSION_CODEC).toUpperCase()),
                conf.getInt(PARQUET_BLOCK_SIZE),
                conf.getInt(PARQUET_PAGE_SIZE));

            Method getBuilder = typeClass.getDeclaredMethod("newBuilder");
            builder = (Message.Builder) getBuilder.invoke(typeClass);
          }
        } catch (Exception e) {
          throw new ConnectException(e);
        }

        Object value = sinkRecord.value();
        try {
          if (value instanceof Struct) {
            byte[] rawJson = converter.fromConnectData(sinkRecord.topic(), sinkRecord.valueSchema(), value);
            parser.merge(new String(rawJson), builder);
          } else {
            parser.merge(value.toString(), builder);
          }
          writer.write(builder.build());
          builder.clear();
        } catch (Exception e) {
          throw new ConnectException(e);
        }
      }

      public void close() {
        try {
          if (writer != null) {
            LOG.info("Start to commit file {}", name);
            writer.close();
            LOG.info("File {} committed", name);
          }
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      public void commit() {
        close();
      }
    };
  }
}
