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
import com.aliyun.oss.connect.kafka.format.bytearray.ByteArrayFormat;
import com.aliyun.oss.connect.kafka.format.json.JsonFormat;
import com.aliyun.oss.connect.kafka.format.parquet.ParquetAvroFormat;
import com.aliyun.oss.connect.kafka.format.parquet.ParquetJsonFormat;
import com.aliyun.oss.connect.kafka.storage.CompressionType;
import com.aliyun.oss.connect.kafka.storage.OSSStorage;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.ComposableConfig;
import io.confluent.connect.storage.common.GenericRecommender;
import io.confluent.connect.storage.common.ParentValueRecommender;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OSSSinkConnectorConfiguration extends StorageSinkConnectorConfig {
  private static final Logger LOG = LoggerFactory.getLogger(OSSSinkConnectorConfiguration.class);

  private final String name;

  private final StorageCommonConfig commonConfig;
  private final HiveConfig hiveConfig;
  private final PartitionerConfig partitionerConfig;
  public static final String OSS_BUCKET = "oss.bucket";

  public static final String FORMAT_BYTEARRAY_EXTENSION_CONFIG = "format.bytearray.extension";
  public static final String FORMAT_BYTEARRAY_EXTENSION_DEFAULT = ".bin";

  public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG = "format.bytearray.separator";
  public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT = System.lineSeparator();

  public static final String COMPRESSION_TYPE_CONFIG = "oss.compression.type";
  public static final String COMPRESSION_TYPE_DEFAULT = "none";

  public static final String PARQUET_PROTOBUF_SCHEMA_CLASS = "oss.parquet.protobuf.schema.class";

  public static final String PARQUET_BLOCK_SIZE = "oss.parquet.block.size";
  public static final int PARQUET_BLOCK_SIZE_DEFAULT = 256 * 1024 * 1024;

  public static final String PARQUET_COMPRESSION_CODEC = "oss.parquet.compression.codec";
  public static final String PARQUET_COMPRESSION_CODEC_DEFAULT = "snappy";

  public static final String PARQUET_PAGE_SIZE = "oss.parquet.page.size";
  public static final int PARQUET_PAGE_SIZE_DEFAULT = 128 * 1024;

  private final Map<String, ComposableConfig> propertyToConfig= new HashMap<>();
  private final Set<AbstractConfig> allConfigs = new HashSet<>();
  private static final GenericRecommender STORAGE_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender FORMAT_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender PARTITIONER_CLASS_RECOMMENDER = new GenericRecommender();
  private static final ParentValueRecommender AVRO_COMPRESSION_RECOMMENDER
      = new ParentValueRecommender(FORMAT_CLASS_CONFIG, AvroFormat.class, AVRO_SUPPORTED_CODECS);

  static {
    STORAGE_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(OSSStorage.class)
    );

    FORMAT_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(
            AvroFormat.class,
            ByteArrayFormat.class,
            JsonFormat.class,
            ParquetAvroFormat.class,
            ParquetJsonFormat.class)
    );

    PARTITIONER_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(
            DefaultPartitioner.class,
            HourlyPartitioner.class,
            DailyPartitioner.class,
            TimeBasedPartitioner.class,
            FieldPartitioner.class
        )
    );
  }
  public OSSSinkConnectorConfiguration(Map<String, String> properties) {
    this(newConfigDef(), properties);
  }

  protected OSSSinkConnectorConfiguration(ConfigDef configDef, Map<String, String> properties) {
    super(configDef, properties);

    this.name = parseName(originalsStrings());

    commonConfig = new StorageCommonConfig(
        StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER), originalsStrings());

    partitionerConfig = new PartitionerConfig(
        PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER), originalsStrings());

    hiveConfig = new HiveConfig(originalsStrings());

    addToGlobal(hiveConfig);
    addToGlobal(partitionerConfig);
    addToGlobal(commonConfig);
    addToGlobal(this);
  }

  public static ConfigDef newConfigDef() {
    ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(
        FORMAT_CLASS_RECOMMENDER, AVRO_COMPRESSION_RECOMMENDER);
    int orderInGroup = 0;
    String group = "OSS";
    if (!configDef.configKeys().containsKey(OSS_BUCKET)) {
      configDef.define(
          OSS_BUCKET,
          ConfigDef.Type.STRING,
          ConfigDef.Importance.HIGH,
          "The OSS Bucket.",
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          "OSS Bucket"
      );
    }

    if (!configDef.configKeys().containsKey(FORMAT_BYTEARRAY_EXTENSION_CONFIG)) {
      configDef.define(
          FORMAT_BYTEARRAY_EXTENSION_CONFIG,
          ConfigDef.Type.STRING,
          FORMAT_BYTEARRAY_EXTENSION_DEFAULT,
          ConfigDef.Importance.LOW,
          String.format(
              "Output file extension for ByteArrayFormat. Defaults to '%s'",
              FORMAT_BYTEARRAY_EXTENSION_DEFAULT
          ),
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          "Output file extension for ByteArrayFormat"
      );
    }

    if (!configDef.configKeys().containsKey(COMPRESSION_TYPE_CONFIG)) {
      configDef.define(
          COMPRESSION_TYPE_CONFIG,
          ConfigDef.Type.STRING,
          COMPRESSION_TYPE_DEFAULT,
          new CompressionTypeValidator(),
          ConfigDef.Importance.LOW,
          "Compression type for file written to OSS. "
              + "Applied when using JsonFormat or ByteArrayFormat. "
              + "Available values: none, gzip.",
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          "Compression type"
      );
    }
    if (!configDef.configKeys().containsKey(PARQUET_PROTOBUF_SCHEMA_CLASS)) {
      configDef.define(
          PARQUET_PROTOBUF_SCHEMA_CLASS,
          ConfigDef.Type.LIST,
          Collections.EMPTY_LIST,
          ConfigDef.Importance.LOW,
          "Protobuf schema class for file written to OSS. "
              + "Applied when using ParquetJsonFormat. ",
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          "Protobuf schema class for ParquetJsonFormat"
      );
    }

    if (!configDef.configKeys().containsKey(PARQUET_BLOCK_SIZE)) {
      configDef.define(
          PARQUET_BLOCK_SIZE,
          ConfigDef.Type.INT,
          PARQUET_BLOCK_SIZE_DEFAULT,
          ConfigDef.Importance.LOW,
          "Parquet block size for file written to OSS. "
              + "Applied when using ParquetJsonFormat or ParquetAvroFormat."
              + "Available values: UNCOMPRESSED, SNAPPY, GZIP, LZO.",
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          "Parquet block size"
      );
    }

    if (!configDef.configKeys().containsKey(PARQUET_COMPRESSION_CODEC)) {
      configDef.define(
          PARQUET_COMPRESSION_CODEC,
          ConfigDef.Type.STRING,
          PARQUET_COMPRESSION_CODEC_DEFAULT,
          ConfigDef.Importance.LOW,
          "Parquet compression codec for file written to OSS. "
              + "Applied when using ParquetJsonFormat or ParquetAvroFormat.",
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          "Parquet compression codec"
      );
    }

    if (!configDef.configKeys().containsKey(PARQUET_PAGE_SIZE)) {
      configDef.define(
          PARQUET_PAGE_SIZE,
          ConfigDef.Type.INT,
          PARQUET_PAGE_SIZE_DEFAULT,
          ConfigDef.Importance.LOW,
              "Parquet page size for file written to OSS. "
              + "Applied when using ParquetJsonFormat or ParquetAvroFormat.",
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          "Parquet page size"
      );
    }

    return configDef;
  }

  public String getName() {
    return name;
  }

  protected static String parseName(Map<String, String> props) {
    String nameProp = props.get("name");
    return nameProp != null ? nameProp : "OSS-Sink";
  }

  public AvroDataConfig avroDataConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put("schema.cache.size", this.get("schema.cache.size"));
    props.put("enhanced.avro.schema.support", this.get("enhanced.avro.schema.support"));
    props.put("connect.meta.data", this.get("connect.meta.data"));
    return new AvroDataConfig(props);
  }

  private void addToGlobal(AbstractConfig config) {
    allConfigs.add(config);
    addConfig(config.values(), (ComposableConfig) config);
  }

  private void addConfig(Map<String, ?> parsedProps, ComposableConfig config) {
    for (String key : parsedProps.keySet()) {
      propertyToConfig.put(key, config);
    }
  }

  public Map<String, ?> plainValues() {
    Map<String, Object> values = new HashMap<>();
    for (AbstractConfig config : allConfigs) {
      values.putAll(config.values());
    }

    return values;
  }

  public static ConfigDef getConfig() {
    // Define the names of the configurations we're going to override
    Set<String> skip = new HashSet<>();
    skip.add(StorageSinkConnectorConfig.SHUTDOWN_TIMEOUT_CONFIG);

    // Order added is important, so that group order is maintained
    ConfigDef visible = new ConfigDef();
    addAllConfigKeys(visible, newConfigDef(), skip);
    addAllConfigKeys(visible, StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER), skip);
    addAllConfigKeys(visible, PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER), skip);

    return visible;
  }

  private static void addAllConfigKeys(ConfigDef container, ConfigDef other, Set<String> skip) {
    for (ConfigDef.ConfigKey key : other.configKeys().values()) {
      if (skip != null && !skip.contains(key.name)) {
        List<String> groups = container.groups();
        Map<String, ConfigDef.ConfigKey> configKeys = container.configKeys();
        if (configKeys.containsKey(key.name)) {
          throw new ConfigException("Configuration " + key.name + " is defined twice.");
        }
        if (key.group != null && !groups.contains(key.group)) {
          groups.add(key.group);
        }
        configKeys.put(key.name, key);
      }
    }
  }

  @Override
  public Object get(String key) {
    ComposableConfig config = propertyToConfig.get(key);
    if (config == null) {
      throw new ConfigException(String.format("Unknown configuration '%s'", key));
    }
    return config == this ? super.get(key) : config.get(key);
  }

  public String getByteArrayExtension() {
    return getString(FORMAT_BYTEARRAY_EXTENSION_CONFIG);
  }

  public String getFormatByteArrayLineSeparator() {
    // White space is significant for line separators, but ConfigKey trims it out,
    // so we need to check the originals rather than using the normal machinery.
    if (originalsStrings().containsKey(FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG)) {
      return originalsStrings().get(FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG);
    }
    return FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT;
  }

  private static class CompressionTypeValidator implements ConfigDef.Validator {
    public static final Map<String, CompressionType> TYPES_BY_NAME = new HashMap<>();
    public static final String ALLOWED_VALUES;

    static {
      List<String> names = new ArrayList<>();
      for (CompressionType compressionType : CompressionType.values()) {
        TYPES_BY_NAME.put(compressionType.name, compressionType);
        names.add(compressionType.name);
      }
      ALLOWED_VALUES = Utils.join(names, ", ");
    }

    @Override
    public void ensureValid(String name, Object compressionType) {
      String compressionTypeString = ((String) compressionType).trim();
      if (!TYPES_BY_NAME.containsKey(compressionTypeString)) {
        throw new ConfigException(name, compressionType, "Value must be one of: " + ALLOWED_VALUES);
      }
    }

    @Override
    public String toString() {
      return "[" + ALLOWED_VALUES + "]";
    }
  }

  public CompressionType getCompressionType() {
    return CompressionType.forName(getString(COMPRESSION_TYPE_CONFIG));
  }
}
