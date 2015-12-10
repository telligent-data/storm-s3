/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.s3.output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 *
 */
public enum ContentEncoding {

  NONE(new NoEncoding(), ""), GZIP(new Gzip(), ".gz");

  private final Encoder encoder;
  private final String suffix;

  ContentEncoding(Encoder encode, String suffix) {
    this.encoder = encode;
    this.suffix = suffix;
  }

  OutputStream encode(OutputStream out) throws IOException {
    return encoder.wrap(out);
  }

  public String getSuffix() {
    return this.suffix;
  }

  public interface Encoder{
    OutputStream wrap(OutputStream out) throws IOException;
  }

  private static class NoEncoding implements Encoder{

    @Override
    public OutputStream wrap(OutputStream out) {
      return out;
    }
  }

  private static class Gzip implements Encoder{

    @Override
    public OutputStream wrap(OutputStream out) throws IOException {
      return new GZIPOutputStream(out);
    }
  }
}
