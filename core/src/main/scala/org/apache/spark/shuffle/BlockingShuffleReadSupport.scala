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
package org.apache.spark.shuffle

import java.io.InputStream
import java.util.{Iterator => JIterator}

import scala.collection.JavaConverters._

import org.apache.spark.shuffle.api.{ShufflePartitionReader, ShuffleReadSupport}
import org.apache.spark.storage.{BlockId, ShuffleBlockId, ShuffleDataBlockId}

/**
 * A very simple implementation of ShuffleReadSupport that fetches one block at a time, and doesn't
 * even start requesting the next block until more data is requested
 */
abstract class BlockingShuffleReadSupport extends ShuffleReadSupport {
  def newPartitionReader(appId: String, shuffleId: Int, mapId: Int): ShufflePartitionReader

  override
  def fetchBlocks(
      appId: String,
      shuffleId: Int,
      blocks: JIterator[(Integer, Seq[BlockId])]): JIterator[InputStream] = {
    blocks.asScala.flatMap { case (mapId, blockIds) =>
      val reader = newPartitionReader(appId, shuffleId, mapId)
      // TODO a custom iterator here which increments Shuffle fetch wait time, each time
      // we end up waiting on a block.
      blockIds.map {
        case blockId@ShuffleBlockId(_, _, reduceId) =>
          reader.fetchPartition(reduceId)
        case dataBlockId@ShuffleDataBlockId(_, _, reduceId) =>
          reader.fetchPartition(reduceId)
        case invalid =>
          throw new IllegalArgumentException(s"Invalid block id $invalid")
      }
    }.asJava
  }
}
