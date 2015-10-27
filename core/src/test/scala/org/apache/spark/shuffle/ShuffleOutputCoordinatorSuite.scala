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

import java.io.{FileInputStream, FileOutputStream, File}

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{JavaSerializer, SerializerInstance}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

class ShuffleOutputCoordinatorSuite extends SparkFunSuite with BeforeAndAfterEach {

  var tempDir: File = _
  var mapStatusFile: File = _
  // use the "port" as a way to distinguish mapstatuses, just for the test
  def mapStatus(id: Int) = MapStatus(BlockManagerId("1", "a.b.c", id), Array(0L, 1L))
  def ser: SerializerInstance = new JavaSerializer(new SparkConf()).newInstance()

  override def beforeEach(): Unit = {
    tempDir = Utils.createTempDir()
    mapStatusFile = File.createTempFile("shuffle", ".mapstatus", tempDir)
    mapStatusFile.delete()
  }

  override def afterEach(): Unit = {
    Utils.deleteRecursively(tempDir)
  }

  private def writeFile(filename: String, data: Int): File = {
    val f = new File(tempDir, filename)
    val out = new FileOutputStream(f)
    out.write(data)
    out.close()
    f
  }

  private def verifyFiles(successfulAttempt: Int): Unit = {
    (0 until 3).foreach { idx =>
      val exp = successfulAttempt* 3 + idx
      val file = new File(tempDir, s"d$idx")
      withClue(s"checking dest file $file") {
        assert(file.length === 1)
        val in = new FileInputStream(file)
        assert(in.read() === exp)
        in.close()

      }
    }
  }

  private def generateAttempt(attempt: Int): Seq[(File, File)] = {
    (0 until 3).map { idx =>
      val j = attempt * 3 + idx
      writeFile(s"t$j", j) -> new File(tempDir, s"d$idx")
    }
  }

  private def commit(files: Seq[(File, File)], id: Int): (Boolean, MapStatus) = {
    ShuffleOutputCoordinator.commitOutputs(0, 0, files, mapStatus(id), mapStatusFile, ser)
  }

  test("move files if dest missing") {
    val firstAttempt = generateAttempt(0)
    val firstCommit = commit(firstAttempt, 1)
    assert(firstCommit._1)
    assert(firstCommit._2.location.port === 1)
    verifyFiles(0)
    firstAttempt.foreach{ case (t, d) => assert(!t.exists())}

    val secondAttempt = generateAttempt(1)
    // second commit fails, and also deletes the tmp files
    val secondCommit = commit(secondAttempt, 2)
    assert(!secondCommit._1)
    // still the mapstatus from the first commit
    assert(firstCommit._2.location.port === 1)
    verifyFiles(0)
    // make sure we delete the temp files if the dest exists
    secondAttempt.foreach{ case (t, d) => assert(!t.exists())}
  }

  test("move files if just map status file missing") {
    val firstAttempt = generateAttempt(0)
    val firstCommit = commit(firstAttempt, 1)
    assert(firstCommit._1)
    assert(firstCommit._2.location.port === 1)
    verifyFiles(0)
    firstAttempt.foreach{ case (t, d) => assert(!t.exists())}

    val secondAttempt = generateAttempt(1)
    mapStatusFile.delete()
    // second commit now succeeds since one destination file is missing
    val secondCommit = commit(secondAttempt, 2)
    assert(secondCommit._1)
    assert(secondCommit._2.location.port === 2)
    verifyFiles(1)
    secondAttempt.foreach{ case (t, d) => assert(!t.exists())}
  }

  test("no missing tmp files") {
    val firstAttempt = generateAttempt(0) ++
      Seq(new File(tempDir, "bogus") -> new File(tempDir, "blah"))
    val ex = intercept[IllegalArgumentException] {commit(firstAttempt, 1)}
    assert(ex.getMessage.contains("Cannot commit non-existent shuffle output"))
  }

}
