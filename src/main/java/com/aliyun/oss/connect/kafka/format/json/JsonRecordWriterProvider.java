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

package com.aliyun.oss.connect.kafka.format.json;

import com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration;
import com.aliyun.oss.connect.kafka.storage.OSSStorage;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class JsonRecordWriterProvider implements RecordWriterProvider<OSSSinkConnectorConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(JsonRecordWriterProvider.class);

  private static final String EXTENSION = ".json";
  private static final String LINE_SEPARATOR = System.lineSeparator();
  private static final byte[] LINE_SEPARATOR_BYTES = LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8);
  private final OSSStorage storage;
  private final ObjectMapper mapper;
  private final JsonConverter converter;

  public JsonRecordWriterProvider(OSSStorage storage, JsonConverter converter) {
    this.storage = storage;
    this.mapper = new ObjectMapper();
    this.converter = converter;
  }

  @Override
  public String getExtension() {
    return EXTENSION + storage.conf().getCompressionType().extension;
  }

  @Override
  public RecordWriter getRecordWriter(final OSSSinkConnectorConfiguration conf, final String filename) {
    try {
      return new RecordWriter() {
        final OutputStream ossOut = storage.create(filename, conf, true);
        final OutputStream wrappedOSSOut = storage.conf().getCompressionType().wrapForOutput(ossOut);

        final JsonGenerator writer = mapper.getFactory()
            .createGenerator(wrappedOSSOut).setRootValueSeparator(null);

        @Override
        public void write(SinkRecord sinkRecord) {
          try {
            Object value = sinkRecord.value();
            if (value instanceof Struct) {
              byte[] rawJson = converter.fromConnectData(sinkRecord.topic(), sinkRecord.valueSchema(), value);
              wrappedOSSOut.write(rawJson);
              wrappedOSSOut.write(LINE_SEPARATOR_BYTES);
            } else {
              writer.writeObject(value);
              writer.writeRaw(LINE_SEPARATOR);
            }
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        @Override
        public void close() {
          try {
            writer.close();
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        @Override
        public void commit() {
          try {
            LOG.info("Start to commit file {}", filename);
            writer.flush();
            wrappedOSSOut.close();
            LOG.info("File {} committed", filename);
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }
      };
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }
}
