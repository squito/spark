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

package org.apache.spark.shuffle.sort.io

import java.io._
import java.math.BigInteger
import java.nio.ByteBuffer

import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mock
import org.mockito.Mockito.{doAnswer, doNothing, when}
import org.mockito.MockitoAnnotations
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.util.ByteBufferInputStream
import org.apache.spark.util.Utils

class LocalDiskShuffleMapOutputWriterSuite extends SparkFunSuite with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockResolver: IndexShuffleBlockResolver = _
  @Mock(answer = RETURNS_SMART_NULLS) private var shuffleWriteMetrics: ShuffleWriteMetrics = _

  private val NUM_PARTITIONS = 10
  private val data: Array[Array[Int]] = (0 until NUM_PARTITIONS).map { partition =>
    if (partition % 3 == 0) {
      Array[Int]()
    } else {
      (partition until (partition * 3)).toArray
    }
  }.toArray
  val dataLengthPerPartitionInBytes = data.map(_.length * 4)

  private var tempFile: File = _
  private var mergedOutputFile: File = _
  private var tempDir: File = _
  private var partitionSizesInMergedFile: Array[Long] = _
  private var conf: SparkConf = _
  private var mapOutputWriter: LocalDiskShuffleMapOutputWriter = _

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  override def beforeEach(): Unit = {
    MockitoAnnotations.initMocks(this)
    tempDir = Utils.createTempDir(null, "test")
    mergedOutputFile = File.createTempFile("mergedoutput", "", tempDir)
    tempFile = File.createTempFile("tempfile", "", tempDir)
    partitionSizesInMergedFile = null
    conf = new SparkConf()
      .set("spark.app.id", "example.spark.app")
      .set("spark.shuffle.unsafe.file.output.buffer", "16k")
    when(blockResolver.getDataFile(anyInt, anyInt)).thenReturn(mergedOutputFile)

    doNothing().when(shuffleWriteMetrics).incWriteTime(anyLong)

    doAnswer { invocationOnMock =>
      partitionSizesInMergedFile = invocationOnMock.getArguments()(2).asInstanceOf[Array[Long]]
      val tmp: File = invocationOnMock.getArguments()(3).asInstanceOf[File]
      if (tmp != null) {
        mergedOutputFile.delete
        tmp.renameTo(mergedOutputFile)
      }
      null
    }.when(blockResolver)
      .writeIndexFileAndCommit(anyInt, anyInt, any(classOf[Array[Long]]), any(classOf[File]))
    mapOutputWriter = new LocalDiskShuffleMapOutputWriter(
      0,
      0,
      NUM_PARTITIONS,
      shuffleWriteMetrics,
      blockResolver,
      conf)
  }

  private def readRecordsFromFile(): Array[Array[Int]] = {
    val result = new Array[Array[Int]](NUM_PARTITIONS)
    logWarning(s"mergedOutputFile.length = ${mergedOutputFile.length()}")
    Utils.tryWithResource(new DataInputStream(new FileInputStream(mergedOutputFile))) { in =>
      (0 until NUM_PARTITIONS).foreach { p =>
        val partitionSize = partitionSizesInMergedFile(p).toInt / 4
        val inner = new Array[Int](partitionSize)
        (0 until partitionSize).foreach { idx =>
          logWarning(s"about to read idx $idx for partition $p")
          inner(idx) = in.readInt()
        }
        result(p) = inner
      }
    }
    result
  }

  test("writing to an outputstream") {
    (0 until NUM_PARTITIONS).foreach { p =>
      val writer = mapOutputWriter.getPartitionWriter(p)
      val stream = new DataOutputStream(writer.openStream())
      data(p).foreach { i => stream.writeInt(i) }
      stream.close()
      intercept[IllegalStateException] {
        stream.write(p)
      }
      assert(writer.getNumBytesWritten === dataLengthPerPartitionInBytes(p))
    }
    mapOutputWriter.commitAllPartitions()
    assert(partitionSizesInMergedFile === dataLengthPerPartitionInBytes)
    assert(mergedOutputFile.length() === dataLengthPerPartitionInBytes.sum)
    assert(data === readRecordsFromFile())
  }

  test("writing to a channel") {
    (0 until NUM_PARTITIONS).foreach { p =>
      Utils.tryWithResource(new DataOutputStream(new FileOutputStream(tempFile))) { out =>
        data(p).foreach { i => out.writeInt(i) }
      }
      val writer = mapOutputWriter.getPartitionWriter(p)
      val channel = writer.openTransferrableChannel()
      Utils.tryWithResource(new FileInputStream(tempFile)) { tempFileInput =>
        Utils.tryWithResource(writer.openTransferrableChannel()) { channel =>
          channel.transferFrom(tempFileInput.getChannel, 0L, data(p).length * 4)
          assert(writer.getNumBytesWritten === dataLengthPerPartitionInBytes(p))
        }
      }
    }
    mapOutputWriter.commitAllPartitions()
    assert(partitionSizesInMergedFile === dataLengthPerPartitionInBytes)
    assert(mergedOutputFile.length() === dataLengthPerPartitionInBytes.sum)
    assert(data === readRecordsFromFile())
  }

  test("copyStreams with an outputstream") {
    (0 until NUM_PARTITIONS).foreach { p =>
      Utils.tryWithResource(new DataOutputStream(new FileOutputStream(tempFile))) { out =>
        data(p).foreach { i => out.writeInt(i) }
      }
      val writer = mapOutputWriter.getPartitionWriter(p)
      Utils.tryWithResource(new FileInputStream(tempFile)) { tempFileInput =>
        Utils.tryWithResource(writer.openStream()) { stream =>
          Utils.copyStream(tempFileInput, stream, false, false)
          assert(writer.getNumBytesWritten === dataLengthPerPartitionInBytes(p))
        }
      }
    }
    mapOutputWriter.commitAllPartitions()
    assert(partitionSizesInMergedFile === dataLengthPerPartitionInBytes)
    assert(mergedOutputFile.length() === dataLengthPerPartitionInBytes.sum)
    assert(data === readRecordsFromFile())
  }
}
