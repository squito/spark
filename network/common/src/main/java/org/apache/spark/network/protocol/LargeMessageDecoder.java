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
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.spark.network.buffer.LargeByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Decoder used by the client side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 */
@ChannelHandler.Sharable
public final class LargeMessageDecoder extends MessageToMessageDecoder<LargeByteBuf> {

  private final Logger logger = LoggerFactory.getLogger(LargeMessageDecoder.class);
  @Override
  public void decode(ChannelHandlerContext ctx, LargeByteBuf in, List<Object> out) {
    in.setupForReading();//HACK
    LargeFrameMessage.Type msgType = LargeFrameMessage.Type.decode(in);
    LargeFrameMessage decoded = decode(msgType, in);
    assert decoded.type() == msgType;
    logger.trace("Received message " + msgType + ": " + decoded);
    out.add(decoded);
  }

  private LargeFrameMessage decode(LargeFrameMessage.Type msgType, LargeByteBuf in) {
    switch (msgType) {
      case LargeChunkFetchSuccess:
        return LargeChunkFetchSuccess.decode(in);

      default:
        throw new IllegalArgumentException("Unexpected message type: " + msgType);
    }
  }
}
