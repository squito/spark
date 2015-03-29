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
package org.apache.spark.storage

import java.io.{OutputStream, InputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag
import scala.util.Random

import org.scalatest.{Matchers, FunSuite}

import org.apache.spark.serializer.{SerializationStream, DeserializationStream, SerializerInstance}

class MultiBlockSerializerSuite extends FunSuite with Matchers {

  val rng = new Random()
  val data = Seq(3,3,3,3,10,5,4,1,1,10).map{length =>
    val arr = new Array[Byte](length)
    rng.nextBytes(arr)
    arr
  }

  test("blockify") {
    val ser = new MultiBlockSerializer(new DummySerializer, 10)
    data.foreach{arr => ser.writeObject(arr)}

    ser.blockEndpoints should be (Seq(9, 12, 22, 32, 33))
    val blocks = ser.toBlocks
    blocks.size should be (6)
    val exp = Seq(
      0 to 2,
      Seq(3),
      Seq(4),
      5 to 7,
      Seq(8),
      Seq(9)
    )
    blocks.zipWithIndex.foreach { case (bytes, idx) =>
      val expBytes = exp(idx).map { idx => data(idx)}.reduce { _ ++ _ }
      bytes should be (expBytes)
    }
  }

  test("blockify with multiple chunks") {
    val chunkSize = 5
    val blockSize = 14

    val ser = new MultiBlockSerializer(new DummySerializer, maxBlockSize = blockSize,
      chunkSize = chunkSize)
    data.foreach{arr => ser.writeObject(arr)}

    ser.blockEndpoints should be (Seq(12, 22, 33))
    val blocks = ser.toBlocks
    blocks.size should be (4)
    val exp = Seq(
      0 to 3,
      Seq(4),
      5 to 8,
      Seq(9)
    )
    blocks.zipWithIndex.foreach { case (bytes, idx) =>
      val expBytes = exp(idx).map { idx => data(idx)}.reduce { _ ++ _ }
      bytes should be (expBytes)
    }
  }

  test("error on records too large") {
    def t(size: Int, maxSize: Int): String = {
      val ser = new MultiBlockSerializer(new DummySerializer, maxSize)

      val exc = intercept[RecordTooLargeException]{
        val arr = new Array[Byte](size)
        ser.writeObject(arr)
      }
      exc.getMessage()
    }

    t(5123, 2078) should
      be ("Tried to write a record of size 5123 B, which exceeds the max size of 2078 B by 3.0 KB")

    t(5127, 5120) should
      be ("Tried to write a record of size 5127 B, which exceeds the max size of 5120 B by 7.0 B")
  }
}

class DummySerializerStream(val out: OutputStream) extends SerializationStream {
  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    t match {
      case arr: Array[Byte] => out.write(arr)
    }
    this
  }

  override def flush(): Unit = {
    out.flush()
  }

  override def close(): Unit = {
    out.close()
  }
}

class DummySerializer extends SerializerInstance {
  override def serializeStream(s: OutputStream): SerializationStream = new DummySerializerStream(s)

  override def serialize[T: ClassTag](t: T): ByteBuffer = ???

  override def deserializeStream(s: InputStream): DeserializationStream = ???

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = ???

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = ???
}
