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

package org.apache.spark.network;

import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.spark.network.protocol.*;
import org.apache.spark.network.util.NettyUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LargeProtocolSuite {
  private void testServerToClient(LargeFrameMessage msg) {
    EmbeddedChannel serverChannel = new EmbeddedChannel(new LargeMessageEncoder());
    serverChannel.writeOutbound(msg);

    EmbeddedChannel clientChannel = new EmbeddedChannel(
        new FixedChunkLargeFrameDecoder(65536), new LargeMessageDecoder());

    while (!serverChannel.outboundMessages().isEmpty()) {
      clientChannel.writeInbound(serverChannel.readOutbound());
    }

    assertEquals(1, clientChannel.inboundMessages().size());
    assertEquals(msg, clientChannel.readInbound());
  }

  private void testClientToServer(Message msg) {
    EmbeddedChannel clientChannel = new EmbeddedChannel(new MessageEncoder());
    clientChannel.writeOutbound(msg);

    EmbeddedChannel serverChannel = new EmbeddedChannel(
        NettyUtils.createFrameDecoder(), new MessageDecoder());

    while (!clientChannel.outboundMessages().isEmpty()) {
      serverChannel.writeInbound(clientChannel.readOutbound());
    }

    assertEquals(1, serverChannel.inboundMessages().size());
    assertEquals(msg, serverChannel.readInbound());
  }

  @Test
  public void requests() {
  }

  @Test
  public void responses() {
    testServerToClient(new LargeChunkFetchSuccess(new StreamChunkId(1, 2),
      new TestLargeManagedBuffer(10)));
    testServerToClient(new LargeChunkFetchSuccess(new StreamChunkId(1, 2),
      new TestLargeManagedBuffer(0)));
    testServerToClient(new LargeChunkFetchSuccess(new StreamChunkId(1, 2),
      new TestLargeManagedBuffer(1000000)));  //over the decoder chunk size
  }
}
