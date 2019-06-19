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

import com.aliyun.oss.connect.kafka.storage.CompressionType;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class JsonUtils {
  private static final ObjectMapper mapper = new ObjectMapper();

  public static Collection<Object> getRecords(
      String bucketName, String fileKey,
      CompressionType compression) throws IOException {
    Path file = new Path("oss://" + bucketName + "/" + fileKey);
    FSDataInputStream stream = file.getFileSystem(new Configuration()).open(file);
    JsonParser reader = mapper.getFactory().createParser(
        compression.wrapForInput(stream.getWrappedStream()));

    ArrayList<Object> records = new ArrayList<>();
    Iterator<Object> iterator = reader.readValuesAs(Object.class);
    while (iterator.hasNext()) {
      records.add(iterator.next());
    }
    stream.close();
    return records;
  }
}
