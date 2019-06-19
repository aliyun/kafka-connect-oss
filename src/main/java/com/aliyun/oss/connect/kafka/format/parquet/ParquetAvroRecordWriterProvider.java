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
import com.aliyun.oss.connect.kafka.utils.Util;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration.OSS_BUCKET;
import static com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration.PARQUET_BLOCK_SIZE;
import static com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration.PARQUET_COMPRESSION_CODEC;
import static com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration.PARQUET_PAGE_SIZE;

public class ParquetAvroRecordWriterProvider implements RecordWriterProvider<OSSSinkConnectorConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetAvroRecordWriterProvider.class);

  private static final String EXTENSION = ".parquet";
  private final OSSStorage storage;
  private final AvroData avroData;

  ParquetAvroRecordWriterProvider(OSSStorage storage, AvroData avroData) {
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

      ParquetWriter<GenericRecord> writer = null;

      final String name = "oss://"
          + conf.get(OSS_BUCKET)
          + conf.getString(StorageCommonConfig.DIRECTORY_DELIM_CONFIG)
          + filename;
      public void write(SinkRecord sinkRecord) {
        try {
          if (writer == null) {
            storage.delete(filename);

            writer = AvroParquetWriter.<GenericRecord>builder(new Path(name))
                .withSchema(avroData.fromConnectSchema(sinkRecord.valueSchema()))
                .withConf(Util.getConf())
                .withCompressionCodec(CompressionCodecName.valueOf(conf.getString(PARQUET_COMPRESSION_CODEC).toUpperCase()))
                .withRowGroupSize(conf.getInt(PARQUET_BLOCK_SIZE))
                .withPageSize(conf.getInt(PARQUET_PAGE_SIZE))
                .build();
          }

          writer.write((GenericRecord) avroData.fromConnectData(sinkRecord.valueSchema(), sinkRecord.value()));
        } catch (IOException e) {
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
