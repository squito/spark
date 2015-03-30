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

import java.io.{FileOutputStream, BufferedOutputStream, File, OutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.SparkException
import org.apache.spark.serializer.{SerializerInstance, SerializationStream}
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ByteArrayChunkOutputStream

private[storage] abstract class MultiBlockSerializer private[storage](
    baseSerializer: SerializerInstance,
    val maxBlockSize: Int,
    val output: OutputStream) extends SerializationStream {

  val out = new SizeTrackingOutputStream(output)

  private val stream = baseSerializer.serializeStream(out)
  private var lastBlockStart: Long = 0L
  private var lastRecordEnd: Long = 0L
  private val _blockEndpoints = ArrayBuffer[Long]()

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    stream.writeObject(t)
    maybeUpdateBlockEndpoints()
    this
  }

  override def close(): Unit = {
    out.close()
  }

  override def flush(): Unit = {
    out.flush()
  }

  private def maybeUpdateBlockEndpoints(): Unit = {
    val length = out.totalBytesWritten
    if (length - lastBlockStart > maxBlockSize) {
      // we've overflowed the last block.  So make the last block start *before* the record
      // we just wrote
      _blockEndpoints += lastRecordEnd
      lastBlockStart = lastRecordEnd
      if (length - lastRecordEnd > maxBlockSize) {
        throw new RecordTooLargeException(length - lastRecordEnd, maxBlockSize)
      }
    }
    lastRecordEnd = length
  }

  def blockEndpoints: Seq[Long] = {
    _blockEndpoints ++ Seq(out.totalBytesWritten)
  }

}

private[storage] class MultiBlockFileSerializer private[storage](
  baseSerializer: SerializerInstance,
  file: File,
  maxBlockSize: Int) extends MultiBlockSerializer(baseSerializer, maxBlockSize,
  new BufferedOutputStream(new FileOutputStream(file))) {

  def this(baseSerializer: SerializerInstance, file: File) = {
    this(baseSerializer, file, Integer.MAX_VALUE - 1e6.toInt)
  }

}

private[storage] class MultiBlockByteArraySerializer private[storage](
  baseSerializer: SerializerInstance,
  maxBlockSize: Int,
  val bytes: ByteArrayChunkOutputStream) extends MultiBlockSerializer(baseSerializer, maxBlockSize,
  bytes) {

  def this(baseSerializer: SerializerInstance, maxBlockSize: Int, chunkSize: Int) = {
    this(baseSerializer, maxBlockSize, new ByteArrayChunkOutputStream(chunkSize))
  }

  def this(baseSerializer: SerializerInstance, maxBlockSize: Int) = {
    this(baseSerializer, maxBlockSize, 65536)
  }

  def this(baseSerializer: SerializerInstance) = {
    this(baseSerializer, Integer.MAX_VALUE - 1e6.toInt)
  }

  //NOTE: requiring each block to be *one* Array[Byte] requires us to do an extra set of copying
  // here ...
  def toBlocks: Seq[Array[Byte]] = {
    val blocks = ArrayBuffer[Array[Byte]]()
    var prevEnd = 0L
    (blockEndpoints).foreach{endpoint =>
      blocks += bytes.slice(prevEnd, endpoint)
      prevEnd = endpoint
    }
    blocks
  }

}

case class RecordTooLargeException(size: Long, maxSize: Int)
  extends SparkException(s"Tried to write a record of size $size B, which exceeds the max size " +
    s"of $maxSize B by ${Utils.bytesToString(size - maxSize)}")
