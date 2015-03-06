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

import io.netty.buffer.Unpooled;
import org.apache.spark.network.buffer.*;

import java.io.IOException;
import java.util.List;

/**
 * A ManagedBuffer implementation that contains 0, 1, 2, 3, ..., (len-1).
 *
 * Used for testing.
 */
public class TestLargeManagedBuffer extends LargeManagedBuffer {

  private final int len;
  private NettyLargeManagedBuffer underlying;

  public TestLargeManagedBuffer(int len) {
    this.len = len;
    byte[] byteArray = new byte[len];
    for (int i = 0; i < len; i ++) {
      byteArray[i] = (byte) i;
    }
    LargeByteBuf largeBB = new LargeByteBuf();
    largeBB.bufs.add(Unpooled.wrappedBuffer(byteArray));
    this.underlying = new NettyLargeManagedBuffer(largeBB);
  }

  @Override
  public List<? extends Object> convertToNetty() throws IOException {
    return underlying.convertToNetty();
  }

  @Override
  public long size() {
    return underlying.size();
  }

  @Override
  public boolean equals(Object other) {
    NettyLargeManagedBuffer otherNb;
    if (other instanceof TestLargeManagedBuffer) {
      otherNb = ((TestLargeManagedBuffer) other).underlying;
    } else if (other instanceof NettyLargeManagedBuffer) {
      otherNb = (NettyLargeManagedBuffer) other;
    } else {
      return false;
    }

    LargeByteBuf otherLbb = otherNb.byteBuf;
    if (otherLbb.remaining() != len) {
      return false;
    } else {
      for (long i = 0; i < len; i ++) {
        if (otherLbb.get() != (byte) i) {
          System.out.println("unequal at i = " + i);
          return false;
        }
      }
      return true;
    }
  }
}
