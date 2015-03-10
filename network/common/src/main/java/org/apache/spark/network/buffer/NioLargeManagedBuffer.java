/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.buffer;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link org.apache.spark.network.buffer.ManagedBuffer} backed by {@link java.nio.ByteBuffer}.
 */
public final class NioLargeManagedBuffer extends LargeManagedBuffer {
  private final LargeByteBuffer buf;

  public NioLargeManagedBuffer(LargeByteBuffer buf) {
    this.buf = buf;
  }

  @Override
  public long size() {
    return buf.remaining();
  }

  public LargeByteBuffer nioByteBuffer() throws IOException {
    return buf.duplicate();
  }

  @Override
  public InputStream createInputStream() throws IOException {
    throw new RuntimeException("TODO");
//    return new ByteBufInputStream(Unpooled.wrappedBuffer(buf));
  }

  @Override
  public LargeManagedBuffer retain() {
    return this;
  }

  @Override
  public LargeManagedBuffer release() {
    return this;
  }

  @Override
  public List<? extends Object> convertToNetty() throws IOException {
    List<ByteBuf> nettyBufs = new ArrayList<ByteBuf>();
    for(ByteBuffer bb: buf.nioBuffers()) {
      nettyBufs.add(Unpooled.wrappedBuffer(bb));
    }
    return nettyBufs;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("buf", buf)
      .toString();
  }
}

