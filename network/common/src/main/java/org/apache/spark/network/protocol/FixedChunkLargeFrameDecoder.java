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
package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.FixedLengthFrameDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.spark.network.buffer.LargeByteBuf;

import java.util.Arrays;
import java.util.List;

public class FixedChunkLargeFrameDecoder extends ByteToMessageDecoder {

  public FixedChunkLargeFrameDecoder(int chunkSize) {
    this.chunkSize = chunkSize;
  }

  final int chunkSize;
  Helper subDecoder = null;
  LargeByteBuf buf = new LargeByteBuf();
  boolean hasChunkInfo = false;
  long totalLength;
  long remaining;
  int nextChunkSize;

  protected final void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    if (!hasChunkInfo) {
      if (in.readableBytes() >= 8) {
        totalLength = in.readLong();
        remaining = totalLength - 8;
        hasChunkInfo = true;
        nextChunkSize = (int) Math.min(remaining, chunkSize);
        subDecoder = new Helper(nextChunkSize);
      }
    }

    if (hasChunkInfo) {
      ByteBuf nextChunk = null;
      while ((nextChunk = subDecoder.decode(ctx, in)) != null ){
        buf.bufs.add(nextChunk);
        remaining -= nextChunkSize;
        nextChunkSize = (int) Math.min(remaining, chunkSize);
        if (remaining == 0) {
          out.add(buf);
          buf = new LargeByteBuf();
          return;
        } else {
          subDecoder = new Helper(nextChunkSize);
        }
      }
    }
  }


  //just to expose the decode method
  static class Helper extends FixedLengthFrameDecoder {
    Helper(int frameLength) {
      super(frameLength);
    }

    @Override
    public ByteBuf decode(ChannelHandlerContext ctx, ByteBuf in)
      throws Exception {
      return (ByteBuf) super.decode(ctx, in);
    }
  }

  private String bufString(ByteBuf actual) {
    actual.markReaderIndex();
    byte[] readBytes = new byte[actual.readableBytes()];
    actual.getBytes(0, readBytes);
    actual.resetReaderIndex();
    return Arrays.toString(readBytes);
  }

  }