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

import java.nio.ByteBuffer

import org.apache.commons.io.output.CountingOutputStream

import org.apache.spark.serializer.{SerializationStream, SerializerInstance}
import org.apache.spark.shuffle.ShufflePartitionWriterOutputStream
import org.apache.spark.shuffle.api.{ShuffleMapOutputWriter, ShufflePartitionWriter}

/**
 * Replicates the concept of {@link DiskBlockObjectWriter}, but with some key differences:
 * - Naturally, instead of writing to an output file, this writes to the partition writer plugin.
 * - The SerializerManager doesn't wrap the streams for compression and encryption; this work is
 *   left to the implementation of the underlying implementation of the writer plugin.
 */
private[spark] class ShufflePartitionObjectWriter(
    bufferSize: Int,
    serializerInstance: SerializerInstance,
    mapOutputWriter: ShuffleMapOutputWriter)
    extends PairsWriter {

  // Reused buffer. Experiments should be done with off-heap at some point.
  private val buffer = ByteBuffer.allocate(bufferSize)

  private var currentWriter: ShufflePartitionWriter = _
  private var objectOutputStream: SerializationStream = _
  private var countingStream: CountingOutputStream = _

  def startNewPartition(partitionId: Int): Unit = {
    require(buffer.position() == 0,
      "Buffer was not flushed to the underlying output on the previous partition.")
    buffer.reset()
    currentWriter = mapOutputWriter.newPartitionWriter(partitionId)
    val currentWriterStream = new ShufflePartitionWriterOutputStream(
      currentWriter, buffer, bufferSize)
    countingStream = new CountingOutputStream(currentWriterStream)
    objectOutputStream = serializerInstance.serializeStream(countingStream)
    // TODO I actually think we should include compression & encryption here.  I'd revisit
    // leaving that up to the plugin in a later revision.  Need to make sure the byte counts
    // respect encryption & compression here
  }

  def commitCurrentPartition(): Long = {
    require(objectOutputStream != null, "Cannot commit a partition that has not been started.")
    require(currentWriter != null, "Cannot commit a partition that has not been started.")
    objectOutputStream.close()
    val length = countingStream.getByteCount
    currentWriter.commitAndGetTotalLength()
    buffer.reset()
    currentWriter = null
    objectOutputStream = null
    length
  }

  def abortCurrentPartition(throwable: Exception): Unit = {
    if (objectOutputStream != null) {
      objectOutputStream.close()
    }

    if (currentWriter != null) {
      currentWriter.abort(throwable)
    }
  }

  def write(key: Any, value: Any): Unit = {
    require(objectOutputStream != null, "Cannot write to a partition that has not been started.")
    objectOutputStream.writeKey(key)
    objectOutputStream.writeValue(value)
  }
}
