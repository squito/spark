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

import scala.util.Random

import org.scalatest.{Matchers, FunSuite}

import org.apache.spark.util.SizeEstimator

class ValueChunkerSuite extends FunSuite with Matchers {

  val NORMAL_ERROR = 0.20
  val HIGH_ERROR = 0.30

  test("fixed size chunker") {
    val chunker = new ValueChunker[String](10000)
    (0 until 10000).foreach{_ => chunker += ("a" * 10)}
    val blks = chunker.blocks
    blks.size should be (70)
    blks.foreach{blk =>
      expectWithinError(blk, 10000, NORMAL_ERROR)
      blk.size should be (142)
    }
  }

  test("variable size chunker") {
    val rand = new Random(123456789)
    def randString(minLen: Int, maxLen: Int): String = {
      "a" * (rand.nextInt(maxLen - minLen) + minLen)
    }
    val chunker = new ValueChunker[String](10000)
    (0 until 10000).foreach{_ => chunker += randString(0,100)}
    val blks = chunker.blocks
    blks.foreach{blk =>
      expectWithinError(blk, 10000, NORMAL_ERROR)
    }

  }

  def expectWithinError(obj: AnyRef, estimatedSize: Long, error: Double) {
    val betterEstimatedSize = SizeEstimator.estimate(obj)
    assert(betterEstimatedSize * (1 - error) < estimatedSize,
      s"Estimated size $estimatedSize was less than expected size $betterEstimatedSize")
    assert(betterEstimatedSize * (1 + 2 * error) > estimatedSize,
      s"Estimated size $estimatedSize was greater than expected size $betterEstimatedSize")
  }

}
