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

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.util.collection.SizeTrackingVector

private[storage] class ValueChunker[T: ClassTag](val maxChunkSize: Int) {

  private val chunks = new mutable.ArrayBuffer[SizeTrackingVector[T]]
  private var currentChunk = new SizeTrackingVector[T]

  def +=(t: T): Unit = {
    currentChunk += t
    // NB even though we do this check every time, its still just based on samples of the records,
    // so we could be arbitrarily far off
    if (currentChunk.estimateSize() > maxChunkSize) {
      chunks += currentChunk
      currentChunk = new SizeTrackingVector[T]
    }
  }

  // If we let a block be a *slice* of an array, we could extra copying
  def blocks: Seq[Array[T]] = {
    chunks.map{_.toArray}
  }

}
