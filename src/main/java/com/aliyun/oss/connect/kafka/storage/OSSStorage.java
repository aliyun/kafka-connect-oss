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

package com.aliyun.oss.connect.kafka.storage;

import com.aliyun.oss.connect.kafka.OSSSinkConnectorConfiguration;
import com.aliyun.oss.connect.kafka.utils.Util;
import io.confluent.connect.storage.Storage;
import org.apache.avro.file.SeekableInput;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * OSS implementation of the storage interface for Connect sinks.
 */
public class OSSStorage implements Storage<OSSSinkConnectorConfiguration, FileStatus[]> {
  private static final Logger LOG = LoggerFactory.getLogger(OSSStorage.class);

  private FileSystem fs;
  private String url;
  private OSSSinkConnectorConfiguration config;
  private String bucket;

  public OSSStorage(OSSSinkConnectorConfiguration config, String url) throws IOException {
    this.bucket = config.getString(OSSSinkConnectorConfiguration.OSS_BUCKET);
    fs = FileSystem.get(URI.create("oss://" + bucket), Util.getConf());
    this.config = config;
    this.url = url;
  }

  private Path toPath(String filename) {
    return new Path("oss://" + bucket + "/" + filename);
  }
  @Override
  public boolean exists(String filename) {
    try {
      return fs.exists(toPath(filename));
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public boolean create(String filename) {
    throw new UnsupportedOperationException("Create only with filename is not supported!");
  }

  @Override
  public SeekableInput open(String filename, OSSSinkConnectorConfiguration config) {
    throw new UnsupportedOperationException("File reading is not currently supported in OSS Connector");
  }

  @Override
  public OutputStream create(String filename, OSSSinkConnectorConfiguration config, boolean overwrite) {
    if (!overwrite) {
      throw new UnsupportedOperationException(
          "Creating a file without overwriting is not currently supported in OSS Connector"
      );
    }

    try {
      return fs.create(toPath(filename), true);
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public OutputStream append(String filename) {
    throw new UnsupportedOperationException("Append is not supported!");
  }

  @Override
  public void delete(String path) {
    try {
      fs.delete(toPath(path), true);
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public FileStatus[] list(String path) {
    try {
      return fs.listStatus(toPath(path));
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public void close() {
    try {
      if (fs != null) {
        //LOG.info("Closing Hadoop FileSystem {}", fs.toString());
        //fs.close();
      }
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public String url() {
    return url;
  }

  @Override
  public OSSSinkConnectorConfiguration conf() {
    return config;
  }
}
