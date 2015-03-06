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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Encoder used by the server side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 */
@ChannelHandler.Sharable
public final class LargeMessageEncoder extends MessageToMessageEncoder<LargeFrameMessage> {

  private final Logger logger = LoggerFactory.getLogger(LargeMessageEncoder.class);

  /***
   * Encodes a Message by invoking its encode() method. For non-data messages, we will add one
   * ByteBuf to 'out' containing the total frame length, the message type, and the message itself.
   * In the case of a ChunkFetchSuccess, we will also add the ManagedBuffer corresponding to the
   * data to 'out', in order to enable zero-copy transfer.
   */
  @Override
  public void encode(ChannelHandlerContext ctx, LargeFrameMessage in, List<Object> out) {
    List<? extends Object> body = null;
    long bodyLength = 0;

    // Only ChunkFetchSuccesses have data besides the header.
    // The body is used in order to enable zero-copy transfer for the payload.
    if (in instanceof LargeChunkFetchSuccess) {
      LargeChunkFetchSuccess resp = (LargeChunkFetchSuccess) in;
      try {
        bodyLength = resp.buffer.size();
        body = resp.buffer.convertToNetty();
      } catch (Exception e) {
        // Re-encode this message as BlockFetchFailure.
        logger.error(String.format("Error opening block %s for client %s",
          resp.streamChunkId, ctx.channel().remoteAddress()), e);
        throw new RuntimeException("TODO");
//        encode(ctx, new ChunkFetchFailure(resp.streamChunkId, e.getMessage()), out);
      }
    }

    LargeFrameMessage.Type msgType = in.type();
    // All messages have the frame length, message type, and message itself.
    int headerLength = 8 + msgType.encodedLength() + in.encodedLength();
    long frameLength = headerLength + bodyLength;
    ByteBuf header = ctx.alloc().heapBuffer(headerLength);
    header.writeLong(frameLength);
    msgType.encode(header);
    in.encode(header);
    assert header.writableBytes() == 0;

    out.add(header);
    if (body != null && bodyLength > 0) {
      for (Object o: body){
        out.add(o);
      }
    }
  }
}
