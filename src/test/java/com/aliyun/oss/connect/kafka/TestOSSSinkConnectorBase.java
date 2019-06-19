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

import com.aliyun.oss.connect.kafka.format.avro.AvroUtils;
import com.aliyun.oss.connect.kafka.format.bytearray.ByteArrayUtils;
import com.aliyun.oss.connect.kafka.format.json.JsonUtils;
import com.aliyun.oss.connect.kafka.format.parquet.ParquetAvroFormat;
import com.aliyun.oss.connect.kafka.format.parquet.ParquetUtils;
import com.aliyun.oss.connect.kafka.storage.CompressionType;
import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.common.util.StringUtils;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.schema.SchemaCompatibility;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TestOSSSinkConnectorBase extends StorageSinkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestOSSSinkConnectorBase.class);

  private static final String OSS_TEST_BUCKET_KEY = "test.fs.oss.bucket";
  protected String OSS_TEST_BUCKET;

  protected static final int TEST_FLUSH_SIZE = 100000;
  protected static final int TEST_RECORDS = 345678;
  protected static final long[] VALID_OFFSETS = new long[TEST_RECORDS / TEST_FLUSH_SIZE + 1];

  protected static final Time SYSTEM_TIME = new SystemTime();
  protected OSSSinkConnectorConfiguration connectorConfig;
  protected String topicsDir;
  protected Map<String, Object> parsedConfig;
  protected SchemaCompatibility compatibility;
  public static final String TEST_FILE_DELIM = "_";
  public static final String TEST_DIRECTORY_DELIM = "/";

  @Rule
  public TestWatcher watcher = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      LOG.info("Starting test: {}.{}",
          description.getTestClass().getSimpleName(), description.getMethodName());
    }
  };

  @Override
  protected Map<String, String> createProps() {
    Configuration conf = new Configuration();
    OSS_TEST_BUCKET = conf.get(OSS_TEST_BUCKET_KEY);
    url = "oss://" + OSS_TEST_BUCKET;

    Map<String, String> props = super.createProps();
    props.put(StorageCommonConfig.STORAGE_CLASS_CONFIG, "com.aliyun.oss.connect.kafka.storage.OSSStorage");

    props.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, TEST_DIRECTORY_DELIM);
    props.put(StorageCommonConfig.FILE_DELIM_CONFIG, TEST_FILE_DELIM);
    props.put(OSSSinkConnectorConfiguration.OSS_BUCKET, OSS_TEST_BUCKET);
    props.put(OSSSinkConnectorConfiguration.FORMAT_CLASS_CONFIG, ParquetAvroFormat.class.getName());
    props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, PartitionerConfig.PARTITIONER_CLASS_DEFAULT.getName());
    props.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "int");
    props.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY_'month'=MM_'day'=dd_'hour'=HH");
    props.put(PartitionerConfig.LOCALE_CONFIG, "en");
    props.put(PartitionerConfig.TIMEZONE_CONFIG, "Asia/Shanghai");
    props.put(HiveConfig.HIVE_CONF_DIR_CONFIG, "Asia/Shanghai");
    props.put(OSSSinkConnectorConfiguration.FLUSH_SIZE_CONFIG, TEST_FLUSH_SIZE + "");
    return props;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    connectorConfig = new OSSSinkConnectorConfiguration(properties);
    topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    parsedConfig = new HashMap<>(connectorConfig.plainValues());
    compatibility = StorageSchemaCompatibility.getCompatibility(
        connectorConfig.getString(HiveConfig.SCHEMA_COMPATIBILITY_CONFIG));

    for (int i = 0; i < VALID_OFFSETS.length; ++i) {
      VALID_OFFSETS[i] = TEST_FLUSH_SIZE * i;
    }
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  public static Collection<Object> readRecords(String topicsDir, String directory,
      TopicPartition tp, long startOffset, String extension,
      String zeroPadFormat, String bucketName) throws IOException {
    return readRecords(topicsDir, directory, tp, startOffset, extension, zeroPadFormat, bucketName, false);
  }

  public static Collection<Object> readRecords(String topicsDir, String directory,
      TopicPartition tp, long startOffset, String extension,
      String zeroPadFormat, String bucketName, boolean isProto) throws IOException {
    String fileKey = fileKeyToCommit(topicsDir, directory, tp, startOffset,
        extension, zeroPadFormat);
    CompressionType compression = CompressionType.NONE;
    if (extension.endsWith(".gz")) {
      compression = CompressionType.GZIP;
    }

    if (".parquet".equals(extension)) {
      return readRecordsParquet(bucketName, fileKey, isProto);
    } else if (".avro".equals(extension)) {
      return readRecordsAvro(bucketName, fileKey);
    } else if (extension.startsWith(".json")) {
      return readRecordsJson(bucketName, fileKey, compression);
    } else if (extension.startsWith(".bin")) {
      return readRecordsByteArray(bucketName, fileKey, compression,
          OSSSinkConnectorConfiguration.FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT.getBytes());
    } else if (extension.startsWith(".kafka")) {
      return readRecordsByteArray(bucketName, fileKey, compression,
          "OSS".getBytes());
    } else {
      throw new IllegalArgumentException("Unknown extension: " + extension);
    }
  }

  public static Collection<Object> readRecordsParquet(
      String bucketName, String fileKey, boolean isProto) throws IOException {
    LOG.debug("Reading records from bucket '{}' key '{}': ", bucketName, fileKey);
    return ParquetUtils.getRecords(bucketName, fileKey, isProto);
  }

  public static Collection<Object> readRecordsAvro(
      String bucketName, String fileKey) throws IOException {
    LOG.debug("Reading records from bucket '{}' key '{}': ", bucketName, fileKey);
    return AvroUtils.getRecords(bucketName, fileKey);
  }

  public static Collection<Object> readRecordsJson(
      String bucketName, String fileKey, CompressionType compression) throws IOException {
    LOG.debug("Reading records from bucket '{}' key '{}': ", bucketName, fileKey);
    return JsonUtils.getRecords(bucketName, fileKey, compression);
  }

  public static Collection<Object> readRecordsByteArray(
      String bucketName, String fileKey, CompressionType compression,
      byte[] lineSeparator) throws IOException {
    LOG.debug("Reading records from bucket '{}' key '{}': ", bucketName, fileKey);
    return ByteArrayUtils.getRecords(bucketName, fileKey, compression, lineSeparator);
  }

  public static String fileKey(String topicsPrefix, String keyPrefix, String name) {
    String suffix = keyPrefix + TEST_DIRECTORY_DELIM + name;
    return StringUtils.isNotBlank(topicsPrefix)
        ? topicsPrefix + TEST_DIRECTORY_DELIM + suffix
        : suffix;
  }

  public static String fileKeyToCommit(String topicsPrefix, String dirPrefix,
      TopicPartition tp, long startOffset, String extension, String zeroPadFormat) {
    String name = tp.topic()
        + TEST_FILE_DELIM
        + tp.partition()
        + TEST_FILE_DELIM
        + String.format(zeroPadFormat, startOffset)
        + extension;
    return fileKey(topicsPrefix, dirPrefix, name);
  }
}
