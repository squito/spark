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

package org.apache.spark.executor

import java.io._
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

import org.apache.spark.SparkEnv
import org.apache.spark.internal.{config, Logging}

private[spark] case class ProcfsBasedSystemsMetrics(
    jvmVmemTotal: Long,
    jvmRSSTotal: Long,
    pythonVmemTotal: Long,
    pythonRSSTotal: Long,
    otherVmemTotal: Long,
    otherRSSTotal: Long)

// Some of the ideas here are taken from the ProcfsBasedProcessTree class in hadoop
// project.
private[spark] class ProcfsBasedSystems(procfsDir: String = "/proc/") extends Logging {
  val procfsStatFile = "stat"
  var pageSize = computePageSize()
  var isAvailable: Boolean = isProcfsAvailable
  private val pid: Int = computePid()
  private val ptree: scala.collection.mutable.Map[ Int, Set[Int]] =
    scala.collection.mutable.Map[ Int, Set[Int]]()

  var allMetrics: ProcfsBasedSystemsMetrics = ProcfsBasedSystemsMetrics(0, 0, 0, 0, 0, 0)
  private var latestJVMVmemTotal: Long = 0
  private var latestJVMRSSTotal: Long = 0
  private var latestPythonVmemTotal: Long = 0
  private var latestPythonRSSTotal: Long = 0
  private var latestOtherVmemTotal: Long = 0
  private var latestOtherRSSTotal: Long = 0

  computeProcessTree()

  private def isProcfsAvailable: Boolean = {
    val testing = sys.env.contains("SPARK_TESTING") || sys.props.contains("spark.testing")
    if (testing) {
      return true
    }
    try {
      if (!Files.exists(Paths.get(procfsDir))) {
        return false
      }
    }
    catch {
      case f: FileNotFoundException => return false
    }
    val shouldLogStageExecutorMetrics =
      SparkEnv.get.conf.get(config.EVENT_LOG_STAGE_EXECUTOR_METRICS)
    val shouldLogStageExecutorProcessTreeMetrics =
      SparkEnv.get.conf.get(config.EVENT_LOG_PROCESS_TREE_METRICS)
    shouldLogStageExecutorProcessTreeMetrics && shouldLogStageExecutorMetrics
  }

  private def computePid(): Int = {
    if (!isAvailable) {
      return -1;
    }
    try {
      // This can be simplified in java9:
      // https://docs.oracle.com/javase/9/docs/api/java/lang/ProcessHandle.html
      val cmd = Array("bash", "-c", "echo $PPID")
      val length = 10
      val out: Array[Byte] = Array.fill[Byte](length)(0)
      Runtime.getRuntime.exec(cmd).getInputStream.read(out)
      val pid = Integer.parseInt(new String(out, "UTF-8").trim)
      return pid;
    }
    catch {
      case e: IOException => logDebug("IO Exception when trying to compute process tree." +
        " As a result reporting of ProcessTree metrics is stopped", e)
        isAvailable = false
        return -1
    }
  }

  private def computePageSize(): Long = {
    val testing = sys.env.contains("SPARK_TESTING") || sys.props.contains("spark.testing")
    if (testing) {
      return 0;
    }
    val cmd = Array("getconf", "PAGESIZE")
    val out: Array[Byte] = Array.fill[Byte](10)(0)
    Runtime.getRuntime.exec(cmd).getInputStream.read(out)
    return Integer.parseInt(new String(out, "UTF-8").trim)
  }

  private def computeProcessTree(): Unit = {
    if (!isAvailable) {
      return
    }
    val queue: Queue[Int] = new Queue[Int]()
    queue += pid
    while( !queue.isEmpty ) {
      val p = queue.dequeue()
      val c = getChildPids(p)
      if(!c.isEmpty) {
        queue ++= c
        ptree += (p -> c.toSet)
      }
      else {
        ptree += (p -> Set[Int]())
      }
    }
  }

  private def getChildPids(pid: Int): ArrayBuffer[Int] = {
    try {
      val cmd = Array("pgrep", "-P", pid.toString)
      val input = Runtime.getRuntime.exec(cmd).getInputStream
      val childPidsInByte: mutable.ArrayBuffer[Byte] = new mutable.ArrayBuffer()
      var d = input.read()
      while (d != -1) {
        childPidsInByte.append(d.asInstanceOf[Byte])
        d = input.read()
      }
      input.close()
      val childPids = new String(childPidsInByte.toArray, "UTF-8").split("\n")
      val childPidsInInt: ArrayBuffer[Int] = new ArrayBuffer[Int]()
      for (p <- childPids) {
        if (p != "") {
          childPidsInInt += Integer.parseInt(p)
        }
      }
      childPidsInInt
    } catch {
      case e: IOException => logDebug("IO Exception when trying to compute process tree." +
        " As a result reporting of ProcessTree metrics is stopped", e)
        isAvailable = false
        return new mutable.ArrayBuffer()
    }
  }

  def getProcessInfo(pid: Int): Unit = {
    /*
   * Hadoop ProcfsBasedProcessTree class used regex and pattern matching to retrive the memory
   * info. I tried that but found it not correct during tests, so I used normal string analysis
   * instead. The computation of RSS and Vmem are based on proc(5):
   * http://man7.org/linux/man-pages/man5/proc.5.html
   */
    try {
      val pidDir: File = new File(procfsDir, pid.toString)
      val fReader = new InputStreamReader(
        new FileInputStream(
          new File(pidDir, procfsStatFile)), Charset.forName("UTF-8"))
      val in: BufferedReader = new BufferedReader(fReader)
      val procInfo = in.readLine
      in.close
      fReader.close
      val procInfoSplit = procInfo.split(" ")
      if ( procInfoSplit != null ) {
        val vmem = procInfoSplit(22).toLong
        val rssPages = procInfoSplit(23).toLong
        if (procInfoSplit(1).toLowerCase(Locale.US).contains("java")) {
          latestJVMVmemTotal += vmem
          latestJVMRSSTotal += rssPages
        }
        else if (procInfoSplit(1).toLowerCase(Locale.US).contains("python")) {
          latestPythonVmemTotal += vmem
          latestPythonRSSTotal += rssPages
        }
        else {
        latestOtherVmemTotal += vmem
        latestOtherRSSTotal += rssPages }
      }
    } catch {
      case f: FileNotFoundException => log.debug("There was a problem with reading" +
        " the stat file of the process", f)
    }
  }

  def updateAllMetrics(): Unit = {
    allMetrics = computeAllMetrics
  }

  private def computeAllMetrics(): ProcfsBasedSystemsMetrics = {
    if (!isAvailable) {
      return ProcfsBasedSystemsMetrics(-1, -1, -1, -1, -1, -1)
    }
    computeProcessTree
    val pids = ptree.keySet
    latestJVMRSSTotal = 0
    latestJVMVmemTotal = 0
    latestPythonRSSTotal = 0
    latestPythonVmemTotal = 0
    latestOtherRSSTotal = 0
    latestOtherVmemTotal = 0
    for (p <- pids) {
      getProcessInfo(p)
    }
    ProcfsBasedSystemsMetrics(
      getJVMVirtualMemInfo,
      getJVMRSSInfo,
      getPythonVirtualMemInfo,
      getPythonRSSInfo,
      getOtherVirtualMemInfo,
      getOtherRSSInfo)

  }

  def getOtherRSSInfo(): Long = {
    if (!isAvailable) {
      return -1
    }
    latestOtherRSSTotal*pageSize
  }

  def getOtherVirtualMemInfo(): Long = {
    if (!isAvailable) {
      return -1
    }
    latestOtherVmemTotal
  }

  def getJVMRSSInfo(): Long = {
    if (!isAvailable) {
      return -1
    }
    latestJVMRSSTotal*pageSize
  }

  def getJVMVirtualMemInfo(): Long = {
    if (!isAvailable) {
      return -1
    }
    latestJVMVmemTotal
  }

  def getPythonRSSInfo(): Long = {
    if (!isAvailable) {
      return -1
    }
    latestPythonRSSTotal*pageSize
  }

  def getPythonVirtualMemInfo(): Long = {
    if (!isAvailable) {
      return -1
    }
    latestPythonVmemTotal
  }
}
