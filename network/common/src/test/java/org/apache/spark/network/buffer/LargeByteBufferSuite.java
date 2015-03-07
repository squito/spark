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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class LargeByteBufferSuite {

  static LargeByteBuffer smallWrapped = LargeByteBufferHelper.allocate(100);
  static File testFile;
  static File largeTestFile;

  @BeforeClass
  public static void setup() throws IOException {
    testFile = File.createTempFile("mapped-file", ".bin");
    testFile.deleteOnExit();
    FileOutputStream out = new FileOutputStream(testFile);
    byte[] fileContent = new byte[1024];
    new Random().nextBytes(fileContent);
    out.write(fileContent);
    out.close();

    largeTestFile = File.createTempFile("large-mapped-file", ".bin");
    System.out.println("writing large data to " + largeTestFile);
    largeTestFile.deleteOnExit();
    out = new FileOutputStream(largeTestFile);
    int l = 1 << 20;
    byte[] data = new byte[l];
    for (int i = 0; i < l; i++) {
      data[i] = (byte) i;
    }
    for (int i =0; i < 3000; i++) {
      out.write(data);
      out.flush();
    }
    out.close();
  }

  @AfterClass
  public static void teardown() throws IOException {
    testFile.delete();
    largeTestFile.delete();
  }

  @Test
  public void testDuplicate() throws IOException {
    LargeByteBuffer bb = smallWrapped;
    bb.position(20);
    LargeByteBuffer dup = bb.duplicate();
    assertEquals(20, dup.position());

    FileInputStream fis = new FileInputStream(testFile);
    LargeByteBuffer mapped = LargeByteBufferHelper.mapFile(
      fis.getChannel(), FileChannel.MapMode.READ_ONLY, 0, testFile.length());
    mapped.position(20);
    LargeByteBuffer mappedDup = mapped.duplicate();
    assertEquals(20, mappedDup.position());
    fis.close();


    fis = new FileInputStream(largeTestFile);
    LargeByteBuffer mappedLarge = LargeByteBufferHelper.mapFile(
      fis.getChannel(), FileChannel.MapMode.READ_ONLY, 0, largeTestFile.length());
    mappedLarge.position(20);
    LargeByteBuffer mappedLargeDup = mappedLarge.duplicate();
    assertEquals(20, mappedLargeDup.position());
    assertEquals((byte)20, mappedLarge.get());
    assertEquals((byte)20, mappedLargeDup.get());

    long p = 3000000000L - 50;
    mappedLarge.position(p);
    assertEquals((byte)p, mappedLarge.get());
    fis.close();
  }

  @Test
  public void testPosition() throws IOException {

  }
}
