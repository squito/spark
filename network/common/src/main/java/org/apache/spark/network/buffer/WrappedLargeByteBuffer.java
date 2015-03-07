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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class WrappedLargeByteBuffer implements LargeByteBuffer {

  private final ByteBuffer[] underlying;
  private final Long totalCapacity;
  private final long[] chunkOffsets;

  private long _pos;
  private int currentBufferIdx;
  private ByteBuffer currentBuffer;
  private long limit;


  public WrappedLargeByteBuffer(ByteBuffer[] underlying) {
    this.underlying = underlying;
    long sum = 0l;
    chunkOffsets = new long[underlying.length + 1];
    for (int i = 0; i < underlying.length; i++) {
      chunkOffsets[i] = sum;
      sum += underlying[i].capacity();
    }
    chunkOffsets[underlying.length] = sum;
    totalCapacity = sum;
    _pos = 0l;
    currentBufferIdx = 0;
    currentBuffer = underlying[0];
    limit = totalCapacity;
  }

  @Override
  public long capacity() {
    return totalCapacity;
  }

  @Override
  public void get(byte[] dest, int offset, int length) {
    int moved = 0;
    while (moved < length) {
      int toRead = Math.min(length - moved, currentBuffer.remaining());
      currentBuffer.get(dest, offset + moved, toRead);
      moved += toRead;
      setBufferForPosition(); //this method could be faster w/ a forward search
    }
    _pos += moved;
  }

  @Override
  public byte get() {
    byte r = currentBuffer.get();
    _pos += 1;
    setBufferForPosition(); //this method could be faster w/ a forward search
    return r;
  }

  private void setBufferForPosition() {
    int idx = 1;
    while (idx <= underlying.length) {
      if (chunkOffsets[idx] > _pos) {
        //found the right buffer, its idx - 1
        currentBufferIdx = idx - 1;
        currentBuffer = underlying[currentBufferIdx];
        currentBuffer.position((int) (_pos - chunkOffsets[currentBufferIdx]));
        return;
      }
      idx += 1;
    }
    throw new RuntimeException("position " + _pos + " is past the end of the buffer");
  }

  @Override
  public void put(LargeByteBuffer bytes) {
    throw new RuntimeException("not yet implemented");
  }

  @Override
  public long position() {
    return _pos;
  }

  @Override
  public void position(long newPosition) {
    //XXX check range?
    _pos = newPosition;
    setBufferForPosition();
  }

  @Override
  public long remaining() {
    return limit - _pos;
  }

  @Override
  public WrappedLargeByteBuffer duplicate() {
    ByteBuffer[] duplicates = new ByteBuffer[underlying.length];
    for (int i = 0; i < underlying.length; i++) {
      duplicates[i] = underlying[i].duplicate();
    }
    //we could avoid initializing offsets here w/ some more work
    WrappedLargeByteBuffer dup = new WrappedLargeByteBuffer(duplicates);
    dup.position(_pos);
    dup.limit = limit;
    return dup;
  }

  @Override
  public long limit() {
    return limit;
  }

  @Override
  public void limit(long newLimit) {
    //XXX check range?  set limits in sub buffers?
    limit = newLimit;
  }

  @Override
  public ByteBuffer asByteBuffer() {
    if (underlying.length == 1)
      return underlying[0];
    else {
      throw new RuntimeException("cannot convert to ByteBuffer; has " + underlying.length +
        "buffers");
    }
  }

  @Override
  public List<ByteBuffer> nioBuffers() {
    return Arrays.asList(underlying);
  }
}
