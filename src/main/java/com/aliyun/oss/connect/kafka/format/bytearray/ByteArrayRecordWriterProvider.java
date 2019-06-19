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

package com.aliyun.oss.connect.kafka.format.bytearray;

import com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration;
import com.aliyun.oss.connect.kafka.storage.OSSStorage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class ByteArrayRecordWriterProvider implements RecordWriterProvider<OSSSinkConnectorConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ByteArrayRecordWriterProvider.class);
  private final OSSStorage storage;
  private final ByteArrayConverter converter;
  private final String extension;
  private final byte[] lineSeparatorBytes;

  public ByteArrayRecordWriterProvider(OSSStorage storage, ByteArrayConverter converter) {
    this.storage = storage;
    this.converter = converter;
    this.extension = storage.conf().getByteArrayExtension();
    this.lineSeparatorBytes = storage.conf()
        .getFormatByteArrayLineSeparator()
        .getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public String getExtension() {
    return extension + storage.conf().getCompressionType().extension;
  }

  @Override
  public RecordWriter getRecordWriter(final OSSSinkConnectorConfiguration conf, final String filename) {
    return new RecordWriter() {
      final OutputStream ossOut = storage.create(filename, conf, true);
      final OutputStream wrappedOSSOut = storage.conf().getCompressionType().wrapForOutput(ossOut);

      @Override
      public void write(SinkRecord sinkRecord) {
        try {
          byte[] bytes = converter.fromConnectData(
              sinkRecord.topic(), sinkRecord.valueSchema(), sinkRecord.value());
          wrappedOSSOut.write(bytes);
          wrappedOSSOut.write(lineSeparatorBytes);
        } catch (IOException | DataException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void close() {
      }

      @Override
      public void commit() {
        try {
          LOG.info("Start to commit file {}", filename);
          wrappedOSSOut.close();
          LOG.info("File {} committed", filename);
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }
    };
  }
}
