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
package org.apache.spark.scheduler

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.internal.Logging
import org.apache.spark.util.Clock

/**
 * Handles blacklisting executors and nodes within a taskset.  This includes blacklisting specific
 * (task, executor) / (task, nodes) pairs, and also completely blacklisting executors and nodes
 * for the entire taskset.
 *
 * THREADING:  This class is a helper to [[TaskSetManager]]; as with the methods in
 * [[TaskSetManager]] this class is designed only to be called from code with a lock on the
 * TaskScheduler (e.g. its event handlers). It should not be called from other threads.
 */
private[scheduler] class TaskSetBlacklist(val conf: SparkConf, val stageId: Int, val clock: Clock)
    extends Logging {

  private val MAX_TASK_ATTEMPTS_PER_EXECUTOR = conf.get(config.MAX_TASK_ATTEMPTS_PER_EXECUTOR)
  private val MAX_TASK_ATTEMPTS_PER_NODE = conf.get(config.MAX_TASK_ATTEMPTS_PER_NODE)
  private val MAX_FAILURES_PER_EXEC_STAGE = conf.get(config.MAX_FAILURES_PER_EXEC_STAGE)
  private val MAX_FAILED_EXEC_PER_NODE_STAGE = conf.get(config.MAX_FAILED_EXEC_PER_NODE_STAGE)
  private val TIMEOUT_MILLIS = BlacklistTracker.getBlacklistTimeout(conf)

  /**
   * A map from each executor to the task failures on that executor.
   */
  val execToFailures: HashMap[String, ExecutorFailuresInTaskSet] = new HashMap()

  /**
   * Map from node to all executors on it with failures.  Needed because we want to know about
   * executors on a node even after they have died. (We don't want to bother tracking the
   * node -> execs mapping in the usual case when there aren't any failures).
   */
  private val nodeToExecsWithFailures: HashMap[String, HashSet[String]] = new HashMap()
  private val nodeToBlacklistedTasks: HashMap[String, HashSet[Int]] = new HashMap()
  private val blacklistedExecs: HashSet[String] = new HashSet()
  private val blacklistedNodes: HashSet[String] = new HashSet()

  /**
   * Return true if this executor is blacklisted for the given task.  This does *not*
   * need to return true if the executor is blacklisted for the entire stage.
   * That is to keep this method as fast as possible in the inner-loop of the
   * scheduler, where those filters will have already been applied.
   */
  def isExecutorBlacklistedForTask(
      executorId: String,
      index: Int): Boolean = {
    execToFailures.get(executorId).exists { execFailures =>
      execFailures.getNumTaskFailures(index) >= MAX_TASK_ATTEMPTS_PER_EXECUTOR
    }
  }

  def isNodeBlacklistedForTask(
      node: String,
      index: Int): Boolean = {
    nodeToBlacklistedTasks.get(node).exists(_.contains(index))
  }

  /**
   * Return true if this executor is blacklisted for the given stage.  Completely ignores
   * anything to do with the node the executor is on.  That
   * is to keep this method as fast as possible in the inner-loop of the scheduler, where those
   * filters will already have been applied.
   */
  def isExecutorBlacklistedForTaskSet(executorId: String): Boolean = {
    blacklistedExecs.contains(executorId)
  }

  def isNodeBlacklistedForTaskSet(node: String): Boolean = {
    blacklistedNodes.contains(node)
  }

  private[scheduler] def updateBlacklistForFailedTask(
      host: String,
      exec: String,
      index: Int): Unit = {
    val execFailures = execToFailures.getOrElseUpdate(exec, new ExecutorFailuresInTaskSet(host))
    execFailures.updateWithFailure(
      taskIndex = index,
      failureExpiryTime = clock.getTimeMillis() + TIMEOUT_MILLIS)

    // check if this task has also failed on other executors on the same host -- if its gone
    // over the limit, blacklist this task from the entire host.
    val execsWithFailuresOnNode = nodeToExecsWithFailures.getOrElseUpdate(host, new HashSet())
    execsWithFailuresOnNode += exec
    val failuresOnHost = execsWithFailuresOnNode.toIterator.flatMap { exec =>
      execToFailures.get(exec).map { failures =>
        // We count task attempts here, not the number of unique executors with failures.  This is
        // because jobs are aborted based on the number task attempts; if we counted unique
        // executors, it would be hard to config to ensure that you try another
        // node before hitting the max number of task failures.
        failures.getNumTaskFailures(index)
      }
    }.sum
    if (failuresOnHost >= MAX_TASK_ATTEMPTS_PER_NODE) {
      nodeToBlacklistedTasks.getOrElseUpdate(host, new HashSet()) += index
    }

    // Check if enough tasks have failed on the executor to blacklist it for the entire stage.
    if (execFailures.numUniqueTasksWithFailures >= MAX_FAILURES_PER_EXEC_STAGE) {
      if (blacklistedExecs.add(exec)) {
        logInfo(s"Blacklisting executor ${exec} for stage $stageId")
        // This executor has been pushed into the blacklist for this stage.  Let's check if it
        // pushes the whole node into the blacklist.
        val blacklistedExecutorsOnNode =
          execsWithFailuresOnNode.filter(blacklistedExecs.contains(_))
        if (blacklistedExecutorsOnNode.size >= MAX_FAILED_EXEC_PER_NODE_STAGE) {
          if (blacklistedNodes.add(host)) {
            logInfo(s"Blacklisting ${host} for stage $stageId")
          }
        }
      }
    }
  }
}
