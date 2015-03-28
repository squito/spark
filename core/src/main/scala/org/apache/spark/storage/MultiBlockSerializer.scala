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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.SparkException
import org.apache.spark.serializer.{SerializerInstance, SerializationStream}
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ByteArrayChunkOutputStream

private[spark] class MultiBlockSerializer(
    baseSerializer: SerializerInstance,
    val maxBlockSize: Int = Integer.MAX_VALUE - 1e6.toInt) extends SerializationStream {

  private val out = new ByteArrayChunkOutputStream(65536)
  private val stream = baseSerializer.serializeStream(out)
  private var lastBlockStart: Long = 0L
  private var lastRecordEnd: Long = 0L
  val blockEndpoints = ArrayBuffer[Long]()

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
    if (out.size - lastBlockStart > maxBlockSize) {
      // we've overflowed the last block.  So make the last block start *before* the record
      // we just wrote
      blockEndpoints += lastRecordEnd
      lastBlockStart = lastRecordEnd
      if (out.size - lastRecordEnd > maxBlockSize) {
        throw new RecordTooLargeException(out.size - lastRecordEnd, maxBlockSize)
      }
    }
    lastRecordEnd = out.size
  }

  //NOTE: requiring each block to be *one* Array[Byte] requires us to do an extra set of copying
  // here ...
  def toBlocks: Seq[Array[Byte]] = {
    val blocks = ArrayBuffer[Array[Byte]]()
    var prevEnd = 0L
    (blockEndpoints.iterator ++ Iterator(out.size)).foreach{endpoint =>
      blocks += out.slice(prevEnd, endpoint)
      prevEnd = endpoint
    }
    blocks
  }

}

case class RecordTooLargeException(size: Long, maxSize: Int)
  extends SparkException(s"Tried to write a record of size $size B, which exceeds the max size " +
    s"of $maxSize B by ${Utils.bytesToString(size - maxSize)}")
