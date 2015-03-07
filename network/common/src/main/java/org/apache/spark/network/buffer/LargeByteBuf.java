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

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class LargeByteBuf {
  public ArrayList<ByteBuf> bufs = new ArrayList<ByteBuf>();
  ByteBuf currentBuffer;
  int currentBufferIdx;
  int currentBufferPos;
  long _pos;

  public long readableBytes() {
    long s = 0;
    for (ByteBuf b: bufs) {
      s += b.readableBytes();
    }
    return s;
  }

  public void retain() {
    for (ByteBuf buf: bufs) {
      buf.retain();
    }
  }

  public void release() {
    for (ByteBuf buf: bufs) {
      buf.release();
    }
  }


  public LargeByteBuf duplicate() {
    LargeByteBuf r = new LargeByteBuf();
    for (ByteBuf buf: bufs) {
      r.bufs.add(buf.duplicate());
    }
    r._pos = _pos;
    r.currentBufferPos = currentBufferPos;
    r.currentBufferIdx = currentBufferIdx;
    r.currentBuffer = currentBuffer == null ? null : r.bufs.get(r.currentBufferIdx);
    return r;
  }

  //we use this pattern of grabbing the bytes we want to deal w/ buffer boundaries :(
  // could also do what CompositeByteBuf does
  private byte[] longBytes = new byte[8];
  private ByteBuffer longBuffer = ByteBuffer.wrap(longBytes);
  public long readLong() {
    get(longBytes, 0, 8);
    return longBuffer.getLong(0);
  }

  private byte[] intBytes = new byte[4];
  private ByteBuffer intBuffer = ByteBuffer.wrap(intBytes);
  public int readInt() {
    get(intBytes, 0, 4);
    return intBuffer.getInt(0);
  }

  public long remaining() {
    return readableBytes() - _pos;
  }

  public void setupForReading() {
    // hack
    currentBuffer = bufs.get(0);
  }

  public byte get() {
    byte r = currentBuffer.getByte(currentBufferPos);
    _pos += 1;
    currentBufferPos += 1;
    updateCurrentBuffer();
    return r;
  }

  public void get(byte[] dest, int offset, int length){
    //there is a lot of vagueness in here about readableBytes vs capacity etc.  Need to come
    // up with clear semantics
    int moved = 0;
    while (moved < length) {
      int toRead = Math.min(length - moved, currentBuffer.readableBytes() - currentBufferPos);
      currentBuffer.getBytes(currentBufferPos, dest, offset + moved, toRead);
      currentBufferPos += toRead;
      moved += toRead;
      updateCurrentBuffer();
    }
    _pos += moved;
  }


  private void updateCurrentBuffer() {
    //TODO end conditions?
    while(currentBuffer != null &&  currentBufferPos >= currentBuffer.readableBytes()) {
      currentBufferIdx += 1;
      currentBuffer = currentBufferIdx < bufs.size() ? bufs.get(currentBufferIdx) : null;
      currentBufferPos = 0;
    }
  }


}
