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
package org.apache.spark.util.io

import java.nio.channels.WritableByteChannel

import io.netty.channel.FileRegion
import io.netty.util.AbstractReferenceCounted

import org.apache.spark.internal.Logging


/**
 * This exposes a ChunkedByteBuffer as a netty FileRegion, just to allow sending > 2gb in one netty
 * message.   This is because netty cannot send a ByteBuf > 2g, but it can send a large FileRegion,
 * even though the data is not backed by a file.
 */
private[io] class ChunkedByteBufferFileRegion(
    val chunkedByteBuffer: ChunkedByteBuffer,
    val ioChunkSize: Int) extends AbstractReferenceCounted with FileRegion with Logging {

  private var _transferred: Long = 0
  // this duplicates the original chunks, so we're free to modify the position, limit, etc.
  private val chunks = chunkedByteBuffer.getChunks()
  private val cumLength = chunks.scanLeft(0L) { _ + _.remaining()}
  private val size = cumLength.last
  // Chunk size in bytes

  protected def deallocate: Unit = {}

  override def count(): Long = chunkedByteBuffer.size

  // this is the "start position" of the overall Data in the backing file, not our current position
  override def position(): Long = 0

  override def transferred(): Long = _transferred

  override def transfered(): Long = _transferred

  override def touch(): ChunkedByteBufferFileRegion = this

  override def touch(hint: Object): ChunkedByteBufferFileRegion = this

  override def retain(): FileRegion = {
    super.retain()
    this
  }

  override def retain(increment: Int): FileRegion = {
    super.retain(increment)
    this
  }

  private var currentChunkIdx = 0

  def transferTo(target: WritableByteChannel, position: Long): Long = {
    assert(position == _transferred)
    if (position == size) return 0L
    var keepGoing = true
    var written = 0L
    var currentChunk = chunks(currentChunkIdx)
    var originalLimit = currentChunk.limit()
    while (keepGoing) {
      while (currentChunk.hasRemaining && keepGoing) {
        val ioSize = Math.min(currentChunk.remaining(), ioChunkSize)
        val originalPos = currentChunk.position()
        val originalLimit = currentChunk.limit()
        currentChunk.limit(currentChunk.position() + ioSize)
        val thisWriteSize = target.write(currentChunk)
        currentChunk.limit(originalLimit)
        written += thisWriteSize
        if (thisWriteSize < ioSize) {
          // the channel did not accept our entire write.  We do *not* keep trying -- netty wants
          // us to just stop, and report how much we've written.
          keepGoing = false
        }
      }
      if (keepGoing) {
        // advance to the next chunk (if there are any more)
        currentChunkIdx += 1
        if (currentChunkIdx == chunks.size) {
          keepGoing = false
        } else {
          currentChunk = chunks(currentChunkIdx)
        }
      }
    }
    _transferred += written
    written
  }
}
