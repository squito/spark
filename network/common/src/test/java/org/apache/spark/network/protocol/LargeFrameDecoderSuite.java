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
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.spark.network.buffer.LargeByteBuf;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LargeFrameDecoderSuite {

  @Test
  public void testSmallMsg() {
    EmbeddedChannel serverChannel = new EmbeddedChannel(new LargeFrameDecoder());
    EmbeddedChannel clientChannel = new EmbeddedChannel(new LargeFrameDecoder());

    //our msg will be:
    // 8 byte total length
    // 4 byte num chunks
    // 4 byte chunk 1 length
    // 20 bytes chunk 1 payload
    long totalLength = 8 + 4 + 4 + 20;
    ByteBuf msg = serverChannel.alloc().buffer((int)totalLength);
    msg.writeLong(totalLength);
    msg.writeInt(1);
    msg.writeInt(24); //length of (length + payload of chunk 1)
    byte[] bytes = new byte[20];
    new Random().nextBytes(bytes);
    msg.writeBytes(bytes);

    serverChannel.writeOutbound(msg);

    while (!serverChannel.outboundMessages().isEmpty()) {
      clientChannel.writeInbound(serverChannel.readOutbound());
    }

    assertEquals(1, clientChannel.inboundMessages().size());
    LargeByteBuf received = (LargeByteBuf) clientChannel.readInbound();
    assertEquals(1, received.bufs.size());
    ByteBuf chunk1 = received.bufs.get(0);
    assertEquals(20, chunk1.readableBytes());
    assertBufEquals(bytes, chunk1);
  }


  @Test
  public void testSmallMultiMsg() {
    EmbeddedChannel serverChannel = new EmbeddedChannel(new LargeFrameDecoder());
    EmbeddedChannel clientChannel = new EmbeddedChannel(new LargeFrameDecoder());

    //our msg will be:
    // 8 byte total length
    // 4 byte num chunks
    // 10 chunks of length 100, each w/ 4 byte length written
    long totalLength = 8 + 4 + (100 + 4)*10;
    ByteBuf msg = serverChannel.alloc().buffer((int)totalLength);
    msg.writeLong(totalLength);
    msg.writeInt(10);
    byte[] bytes = new byte[100];

    for(int chunkIndex = 0; chunkIndex < 10; chunkIndex++) {
      msg.writeInt(104); //length of (length + payload of chunk)
      new Random().nextBytes(bytes);
      msg.writeBytes(bytes);
    }
    serverChannel.writeOutbound(msg);

    while (!serverChannel.outboundMessages().isEmpty()) {
      clientChannel.writeInbound(serverChannel.readOutbound());
    }


    //still gets decoded into one msg
    assertEquals(1, clientChannel.inboundMessages().size());
    LargeByteBuf received = (LargeByteBuf) clientChannel.readInbound();
    assertEquals(10, received.bufs.size());
    for (int chunkIndex = 0; chunkIndex < 10; chunkIndex++) {
      ByteBuf chunk = received.bufs.get(chunkIndex);
      assertEquals(100, chunk.readableBytes());
    }
    //just check the last chunk b/c I'm lazy
    assertBufEquals(bytes, received.bufs.get(9));
  }

  @Test
  public void testLargeMsg() {
    // Requires a lot of memory, should be labeled as an integration test
    EmbeddedChannel serverChannel = new EmbeddedChannel(new LargeFrameDecoder());
    EmbeddedChannel clientChannel = new EmbeddedChannel(new LargeFrameDecoder());

    //our msg will be:
    // 8 byte total length
    // 4 byte num chunks
    // 22000 chunks of length 100000, each w/ 4 byte length written
    // this will put us a little over 2GB
    int numChunks = 22000;
    int chunkLength = 100000;
    long totalLength = 8L + 4L + (numChunks * (4L + (long)chunkLength));
    assertTrue(totalLength > Integer.MAX_VALUE);  //make sure we've got a good test case
    ByteBuf header = serverChannel.alloc().buffer(12);
    header.writeLong(totalLength);
    header.writeInt(numChunks);
    serverChannel.writeOutbound(header);
    byte[] chunkBytes = new byte[chunkLength];
    Random r = new Random();
    for (int chunkIndex = 0; chunkIndex < numChunks; chunkIndex++) {
      ByteBuf chunkMsg = serverChannel.alloc().buffer(chunkLength + 4);
      chunkMsg.writeInt(chunkLength + 4);
      r.nextBytes(chunkBytes);
      chunkMsg.writeBytes(chunkBytes);
      serverChannel.writeOutbound(chunkMsg);
    }

    while (!serverChannel.outboundMessages().isEmpty()) {
      clientChannel.writeInbound(serverChannel.readOutbound());
    }



    //still gets decoded into one msg
    assertEquals(1, clientChannel.inboundMessages().size());
    LargeByteBuf received = (LargeByteBuf) clientChannel.readInbound();
    assertEquals(numChunks, received.bufs.size());
    for (int chunkIndex = 0; chunkIndex < numChunks; chunkIndex++) {
      ByteBuf chunk = received.bufs.get(chunkIndex);
      assertEquals(chunkLength, chunk.readableBytes());
    }
    //just check the last chunk b/c I'm lazy
    assertBufEquals(chunkBytes, received.bufs.get(numChunks - 1));
  }

  private void assertBufEquals(byte[] exp, ByteBuf actual) {
    byte[] readBytes = new byte[exp.length];
    actual.getBytes(0, readBytes);
    assertArrayEquals(exp, readBytes);
  }
}
