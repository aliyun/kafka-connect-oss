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
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;

public class ParquetAvroFormat implements Format<OSSSinkConnectorConfiguration, String> {
  private final OSSStorage storage;
  private final AvroData avroData;

  public ParquetAvroFormat(OSSStorage storage) {
    this.storage = storage;
    this.avroData = new AvroData(storage.conf().avroDataConfig());
  }

  @Override
  public RecordWriterProvider<OSSSinkConnectorConfiguration> getRecordWriterProvider() {
    return new ParquetAvroRecordWriterProvider(storage, avroData);
  }

  @Override
  public SchemaFileReader<OSSSinkConnectorConfiguration, String> getSchemaFileReader() {
    throw new UnsupportedOperationException("Reading schemas from OSS is not currently supported");
  }

  @Override
  public HiveFactory getHiveFactory() {
    throw new UnsupportedOperationException("Hive integration is not currently supported in OSS Connector");
  }

  public AvroData getAvroData() {
    return avroData;
  }
}
