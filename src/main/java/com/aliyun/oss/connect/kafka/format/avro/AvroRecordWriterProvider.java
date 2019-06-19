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

package com.aliyun.oss.connect.kafka.format.avro;

import com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration;
import com.aliyun.oss.connect.kafka.storage.OSSStorage;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class AvroRecordWriterProvider implements RecordWriterProvider<OSSSinkConnectorConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
  private static final String EXTENSION = ".avro";
  private final OSSStorage storage;
  private final AvroData avroData;

  AvroRecordWriterProvider(OSSStorage storage, AvroData avroData) {
    this.storage = storage;
    this.avroData = avroData;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final OSSSinkConnectorConfiguration conf, final String filename) {
    return new RecordWriter() {

      final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
      Schema schema = null;
      OutputStream ossOut;

      @Override
      public void write(SinkRecord sinkRecord) {
        if (schema == null) {
          schema = sinkRecord.valueSchema();
          try {
            LOG.info("Opening record writer for: {}", filename);
            ossOut = storage.create(filename, conf, true);
            org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
            writer.setCodec(CodecFactory.fromString(conf.getAvroCodec()));
            writer.create(avroSchema, ossOut);
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        Object value = avroData.fromConnectData(schema, sinkRecord.value());

        try {
          // AvroData wraps primitive types so their schema can be included. We need to unwrap
          // NonRecordContainers to just their value to properly handle these types
          if (value instanceof NonRecordContainer) {
            value = ((NonRecordContainer) value).getValue();
          }
          writer.append(value);
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void close() {
        try {
          LOG.info("Start to commit file {}", filename);
          writer.flush();
          ossOut.close();
          writer.close();
          LOG.info("File {} committed", filename);
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {
        close();
      }
    };
  }
}
