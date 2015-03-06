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

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

public class FixedChunkLargeFrameDecoderSuite {

  @Test
  public void testSmallMsg() {
    EmbeddedChannel serverChannel = new EmbeddedChannel(new FixedChunkLargeFrameDecoder(65536));
    EmbeddedChannel clientChannel = new EmbeddedChannel(new FixedChunkLargeFrameDecoder(65536));

    //our msg will be:
    // 8 byte total length
    // 20 bytes chunk 1 payload
    long totalLength = 8 + 20;
    ByteBuf msg = serverChannel.alloc().buffer((int)totalLength);
    msg.writeLong(totalLength);
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
  public void testSmallMultiMsgSend() {
    testDecoding(10, 100, 65536);
  }

  @Test
  public void testSmallMultiMsgSendReceive() {
    //decode into small chunks, to test decoding
    //intentionally doesn't divide neatly
    testDecoding(10, 100, 72);
  }

  @Test
  public void testLargeMsg() {
    // Requires a lot of memory, should be labeled as an integration test
    // 22000 chunks of length 100000, each w/ 4 byte length written
    // this will put us a little over 2GB
    testDecoding(22000, 100000, 65536);
  }


  private void testDecoding(int nChunks, int chunkLength, int decodeChunkSize) {
    //required b/c of the way we use Random to generate bytes
    assertTrue(chunkLength % 4 == 0);
    assertTrue(decodeChunkSize % 4 == 0);

    EmbeddedChannel serverChannel = new EmbeddedChannel(
      new FixedChunkLargeFrameDecoder(decodeChunkSize));
    EmbeddedChannel clientChannel = new EmbeddedChannel(
      new FixedChunkLargeFrameDecoder(decodeChunkSize));

    sendMsg(serverChannel, nChunks, chunkLength);

    while (!serverChannel.outboundMessages().isEmpty()) {
      clientChannel.writeInbound(serverChannel.readOutbound());
    }

    //always decoded into one msg
    assertEquals(1, clientChannel.inboundMessages().size());
    LargeByteBuf received = (LargeByteBuf) clientChannel.readInbound();
    verifyChunks(nChunks * (long) chunkLength, decodeChunkSize, received);
  }



  private void sendMsg(EmbeddedChannel serverChannel, int nChunks, int chunkLength) {
    long totalLength = 8 + (nChunks*(long)chunkLength);
    ByteBuf header = serverChannel.alloc().buffer(8);
    header.writeLong(totalLength);
    serverChannel.writeOutbound(header);
    byte[] bytes = new byte[chunkLength];
    Random r = new Random(0);
    for(int chunkIndex = 0; chunkIndex < nChunks; chunkIndex++) {
      ByteBuf chunkMsg = serverChannel.alloc().buffer(chunkLength);
      r.nextBytes(bytes);
      chunkMsg.writeBytes(bytes);
      serverChannel.writeOutbound(chunkMsg);
    }
  }

  private void verifyChunks(long expMsgLength, int decodeChunkSize, LargeByteBuf received) {
    assertEquals(expMsgLength, received.readableBytes());
    int nExpDecodeChunks = (int) Math.ceil(expMsgLength / (double)decodeChunkSize);
    assertEquals(nExpDecodeChunks, received.bufs.size());
    Random r = new Random(0);
    long offset = 0L;
    for (int chunkIndex = 0; chunkIndex < received.bufs.size() -1; chunkIndex++) {
      ByteBuf chunk = received.bufs.get(chunkIndex);
      assertEquals(decodeChunkSize, chunk.readableBytes());
      byte[] expBytes = new byte[chunk.readableBytes()];
      r.nextBytes(expBytes);
      assertBufEquals(expBytes, chunk);
      offset += chunk.readableBytes();
    }
    //last chunk has what's left over
    ByteBuf lastChunk = received.bufs.get(received.bufs.size() -1);
    assertEquals(expMsgLength - offset, lastChunk.readableBytes());
    byte[] expBytes = new byte[lastChunk.readableBytes()];
    r.nextBytes(expBytes);
    assertBufEquals(expBytes, lastChunk);
  }

  private void assertBufEquals(byte[] exp, ByteBuf actual) {
    byte[] actualBytes = new byte[exp.length];
    actual.getBytes(0, actualBytes);
    assertArrayEquals(exp, actualBytes);
  }
}
