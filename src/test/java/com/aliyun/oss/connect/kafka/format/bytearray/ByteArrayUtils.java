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

import com.aliyun.oss.connect.kafka.storage.CompressionType;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ByteArrayUtils {
  public static Collection<Object> getRecords(
      String bucketName, String fileKey, CompressionType compression,
      byte[] lineSeparatorBytes) throws IOException {
    Path file = new Path("oss://" + bucketName + "/" + fileKey);
    FSDataInputStream stream = file.getFileSystem(new Configuration()).open(file);
    byte[] bytes = IOUtils.toByteArray(compression.wrapForInput(stream.getWrappedStream()));
    Collection<Object> result = splitLines(bytes, lineSeparatorBytes);
    stream.close();
    return result;
  }

  private static boolean isMatch(byte[] lineSeparatorBytes, byte[] input, int pos) {
    for (int i = 0; i < lineSeparatorBytes.length; i++) {
      if (lineSeparatorBytes[i] != input[pos+i]) {
        return false;
      }
    }
    return true;
  }

  private static Collection<Object> splitLines(byte[] input, byte[] lineSeparatorBytes) {
    List<Object> records = new ArrayList<>();
    int lineStart = 0;
    for (int i = 0; i < input.length; i++) {
      if (isMatch(lineSeparatorBytes, input, i)) {
        records.add(Arrays.copyOfRange(input, lineStart, i));
        lineStart = i + lineSeparatorBytes.length;
        i = lineStart;
      }
    }
    if (lineStart != input.length) {
      records.add(Arrays.copyOfRange(input, lineStart, input.length));
    }
    return records;
  }
}
