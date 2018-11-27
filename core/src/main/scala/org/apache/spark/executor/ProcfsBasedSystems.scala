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

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.Utils

private[spark] case class ProcfsBasedSystemsMetrics(
    jvmVmemTotal: Long,
    jvmRSSTotal: Long,
    pythonVmemTotal: Long,
    pythonRSSTotal: Long,
    otherVmemTotal: Long,
    otherRSSTotal: Long)

// Some of the ideas here are taken from the ProcfsBasedProcessTree class in hadoop
// project.
private[spark] class ProcfsBasedSystems(
    val procfsDir: String,
    val pageSize: Long,
    val testing: Boolean) extends Logging {

  def this() {
    this(
      "/proc/",
      ProcfsBasedSystems.computePageSize(false),
      sys.env.contains("SPARK_TESTING") || sys.props.contains("spark.testing"))
  }

  val procfsStatFile = "stat"
  var isAvailable: Boolean = pageSize > 0 && isProcfsAvailable
  private val pid = computePid()

  // var allMetrics: ProcfsBasedSystemsMetrics = ProcfsBasedSystemsMetrics(0, 0, 0, 0, 0, 0)

  computeProcessTree()

  private lazy val isProcfsAvailable: Boolean = {
    if (testing) {
       true
    }
    else {
      var procDirExists = true
      try {
        if (!Files.exists(Paths.get(procfsDir))) {
          procDirExists = false
        }
      }
      catch {
        case f: IOException =>
          logWarning("It seems that procfs isn't supported", f)
          procDirExists = false
      }
      val shouldLogStageExecutorMetrics =
        SparkEnv.get.conf.get(config.EVENT_LOG_STAGE_EXECUTOR_METRICS)
      val shouldLogStageExecutorProcessTreeMetrics =
        SparkEnv.get.conf.get(config.EVENT_LOG_PROCESS_TREE_METRICS)
      procDirExists && shouldLogStageExecutorProcessTreeMetrics && shouldLogStageExecutorMetrics
    }
  }

  private def computePid(): Int = {
    if (!isAvailable || testing) {
      return -1;
    }
    try {
      // This can be simplified in java9:
      // https://docs.oracle.com/javase/9/docs/api/java/lang/ProcessHandle.html
      val cmd = Array("bash", "-c", "echo $PPID")
      val out2 = Utils.executeAndGetOutput(cmd)
      val pid = Integer.parseInt(out2.split("\n")(0))
      return pid;
    }
    catch {
      case e: SparkException =>
        logWarning("Exception when trying to compute process tree." +
        " As a result reporting of ProcessTree metrics is stopped", e)
        isAvailable = false
        return -1
    }
  }

  private def computeProcessTree(): Set[Int] = {
    if (!isAvailable || testing) {
      return Set()
    }
    var ptree: Set[Int] = Set()
    ptree += pid
    val queue = mutable.Queue.empty[Int]
    queue += pid
    while( !queue.isEmpty ) {
      val p = queue.dequeue()
      val c = getChildPids(p)
      if(!c.isEmpty) {
        queue ++= c
        ptree ++= c.toSet
      }
    }
    ptree
  }

  private def getChildPids(pid: Int): ArrayBuffer[Int] = {
    try {
      // val cmd = Array("pgrep", "-P", pid.toString)
      val builder = new ProcessBuilder("pgrep", "-P", pid.toString)
      val process = builder.start()
      // val output = new StringBuilder()
      val threadName = "read stdout for " + "pgrep"
      val childPidsInInt = mutable.ArrayBuffer.empty[Int]
      def appendChildPid(s: String): Unit = {
        if (s != "") {
          logDebug("Found a child pid:" + s)
          childPidsInInt += Integer.parseInt(s)
        }
      }
      val stdoutThread = Utils.processStreamByLine(threadName,
        process.getInputStream, appendChildPid)
      val exitCode = process.waitFor()
      stdoutThread.join()
      // pgrep will have exit code of 1 if there are more than one child process
      // and it will have a exit code of 2 if there is no child process
      if (exitCode != 0 && exitCode > 2) {
        val cmd = builder.command().toArray.mkString(" ")
        logWarning(s"Process $cmd" +
          s" exited with code $exitCode, with stderr:" + s"${process.getErrorStream} ")
        throw new SparkException(s"Process $cmd exited with code $exitCode")
      }
      childPidsInInt
    } catch {
      case e: Exception => logWarning("Exception when trying to compute process tree." +
        " As a result reporting of ProcessTree metrics is stopped.", e)
        isAvailable = false
        return mutable.ArrayBuffer.empty[Int]
    }
  }

  def computeProcessInfo(allMetrics: ProcfsBasedSystemsMetrics, pid: Int):
  ProcfsBasedSystemsMetrics = {
  /*
   * Hadoop ProcfsBasedProcessTree class used regex and pattern matching to retrive the memory
   * info. I tried that but found it not correct during tests, so I used normal string analysis
   * instead. The computation of RSS and Vmem are based on proc(5):
   * http://man7.org/linux/man-pages/man5/proc.5.html
   */
    try {
      val pidDir = new File(procfsDir, pid.toString)
      var allMetricsUpdated = ProcfsBasedSystemsMetrics(0, 0, 0, 0, 0, 0)
      Utils.tryWithResource( new InputStreamReader(
        new FileInputStream(
          new File(pidDir, procfsStatFile)), Charset.forName("UTF-8"))) { fReader =>
        Utils.tryWithResource( new BufferedReader(fReader)) { in =>
          val procInfo = in.readLine
          val procInfoSplit = procInfo.split(" ")
          if (procInfoSplit != null) {
            val vmem = procInfoSplit(22).toLong
            val rssPages = procInfoSplit(23).toLong
            if (procInfoSplit(1).toLowerCase(Locale.US).contains("java")) {
              allMetricsUpdated = allMetrics.copy(
                jvmVmemTotal = allMetrics.jvmVmemTotal + vmem,
                jvmRSSTotal = allMetrics.jvmRSSTotal + (rssPages*pageSize)
              )
            }
            else if (procInfoSplit(1).toLowerCase(Locale.US).contains("python")) {
              allMetricsUpdated = allMetrics.copy(
                pythonVmemTotal = allMetrics.pythonVmemTotal + vmem,
                pythonRSSTotal = allMetrics.pythonRSSTotal + (rssPages*pageSize)
              )
            }
            else {
              allMetricsUpdated = allMetrics.copy(
                otherVmemTotal = allMetrics.otherVmemTotal + vmem,
                otherRSSTotal = allMetrics.otherRSSTotal + (rssPages*pageSize)
              )
            }
          }
        }
      }
      allMetricsUpdated
    } catch {
      case f: FileNotFoundException => logWarning("There was a problem with reading" +
        " the stat file of the process. ", f)
        ProcfsBasedSystemsMetrics(0, 0, 0, 0, 0, 0)
    }
  }

  private[spark] def computeAllMetrics(): ProcfsBasedSystemsMetrics = {
    if (!isAvailable) {
      return ProcfsBasedSystemsMetrics(0, 0, 0, 0, 0, 0)
    }
    val pids = computeProcessTree
    var allMetrics = ProcfsBasedSystemsMetrics(0, 0, 0, 0, 0, 0)
    for (p <- pids) {
      allMetrics = computeProcessInfo(allMetrics, p)
    }
    return allMetrics
  }
}

private[spark] object ProcfsBasedSystems extends Logging {

  private def computePageSize(testing: Boolean): Long = {
    if (testing) {
      return 0;
    }
    try {
      val cmd = Array("getconf", "PAGESIZE")
      val out = Utils.executeAndGetOutput(cmd)
      Integer.parseInt(out.split("\n")(0))
    } catch {
      case e: Exception =>
        logWarning("Exception when trying to compute pagesize, as a" +
        " result reporting of ProcessTree metrics is stopped")
        return 0
    }
  }

}
