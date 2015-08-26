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

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.*;

public class LargeByteBufferHelperSuite {

  @Test
  public void testMapFile() throws IOException {
    File testFile = File.createTempFile("large-byte-buffer-test", ".bin");
    try {
      testFile.deleteOnExit();
      OutputStream out = new FileOutputStream(testFile);
      byte[] buffer = new byte[1 << 16];
      Random rng = new XORShiftRandom(0L);
      long len = ((long)buffer.length) + Integer.MAX_VALUE + 1;
      for (int i = 0; i < len / buffer.length; i++) {
        rng.nextBytes(buffer);
        out.write(buffer);
      }
      out.close();

      FileChannel in = new FileInputStream(testFile).getChannel();

      //fail quickly on bad bounds
      try {
        LargeByteBufferHelper.mapFile(in, FileChannel.MapMode.READ_ONLY, 0, len + 1);
        fail("expected exception");
      } catch (IOException ioe) {
      }
      try {
        LargeByteBufferHelper.mapFile(in, FileChannel.MapMode.READ_ONLY, -1, 10);
        fail("expected exception");
      } catch (IllegalArgumentException iae) {
      }

      //now try to read from the buffer
      LargeByteBuffer buf = LargeByteBufferHelper.mapFile(in, FileChannel.MapMode.READ_ONLY, 0, len);
      assertEquals(len, buf.size());
      byte[] read = new byte[buffer.length];
      byte[] expected = new byte[buffer.length];
      Random rngExpected = new XORShiftRandom(0L);
      for (int i = 0; i < len / buffer.length; i++) {
        buf.get(read, 0, buffer.length);
        // assertArrayEquals() is really slow
        rngExpected.nextBytes(expected);
        for (int j = 0; j < buffer.length; j++) {
          if (read[j] !=  expected[j])
            fail("bad byte at (i,j) = (" + i + "," + j + ")");
        }
      }
    } finally {
      testFile.delete();
    }
  }

  @Test
  public void testAllocate() {
    WrappedLargeByteBuffer buf = (WrappedLargeByteBuffer) LargeByteBufferHelper.allocate(95,10);
    assertEquals(10, buf.underlying.length);
    for (int i = 0 ; i < 9; i++) {
      assertEquals(10, buf.underlying[i].capacity());
    }
    assertEquals(5, buf.underlying[9].capacity());
  }


  private class XORShiftRandom extends Random {

    XORShiftRandom(long init) {
      super(init);
      seed = new Random(init).nextLong();
    }

    long seed;

    // we need to just override next - this will be called by nextInt, nextDouble,
    // nextGaussian, nextLong, etc.
    @Override
    protected int next(int bits) {
      long nextSeed = seed ^ (seed << 21);
      nextSeed ^= (nextSeed >>> 35);
      nextSeed ^= (nextSeed << 4);
      seed = nextSeed;
      return (int) (nextSeed & ((1L << bits) -1));
    }
  }

}
