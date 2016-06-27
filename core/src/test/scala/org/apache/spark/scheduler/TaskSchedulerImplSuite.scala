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

import org.mockito.Matchers._
import org.mockito.Mockito.{atLeast, never, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mock.MockitoSugar

import org.apache.spark._
import org.apache.spark.internal.Logging

class FakeSchedulerBackend extends SchedulerBackend {
  def start() {}
  def stop() {}
  def reviveOffers() {}
  def defaultParallelism(): Int = 1
}

class TaskSchedulerImplSuite extends SparkFunSuite with LocalSparkContext with Logging
    with MockitoSugar{

  test("Scheduler does not always schedule tasks on the same workers") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }

    val numFreeCores = 1
    val workerOffers = Seq(new WorkerOffer("executor0", "host0", numFreeCores),
      new WorkerOffer("executor1", "host1", numFreeCores))
    // Repeatedly try to schedule a 1-task job, and make sure that it doesn't always
    // get scheduled on the same executor. While there is a chance this test will fail
    // because the task randomly gets placed on the first executor all 1000 times, the
    // probability of that happening is 2^-1000 (so sufficiently small to be considered
    // negligible).
    val numTrials = 1000
    val selectedExecutorIds = 1.to(numTrials).map { _ =>
      val taskSet = FakeTask.createTaskSet(1)
      taskScheduler.submitTasks(taskSet)
      val taskDescriptions = taskScheduler.resourceOffers(workerOffers).flatten
      assert(1 === taskDescriptions.length)
      taskDescriptions(0).executorId
    }
    val count = selectedExecutorIds.count(_ == workerOffers(0).executorId)
    assert(count > 0)
    assert(count < numTrials)
  }

  test("Scheduler correctly accounts for multiple CPUs per task") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskCpus = 2

    sc.conf.set("spark.task.cpus", taskCpus.toString)
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }
    // Give zero core offers. Should not generate any tasks
    val zeroCoreWorkerOffers = Seq(new WorkerOffer("executor0", "host0", 0),
      new WorkerOffer("executor1", "host1", 0))
    val taskSet = FakeTask.createTaskSet(1)
    taskScheduler.submitTasks(taskSet)
    var taskDescriptions = taskScheduler.resourceOffers(zeroCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // No tasks should run as we only have 1 core free.
    val numFreeCores = 1
    val singleCoreWorkerOffers = Seq(new WorkerOffer("executor0", "host0", numFreeCores),
      new WorkerOffer("executor1", "host1", numFreeCores))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(singleCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // Now change the offers to have 2 cores in one executor and verify if it
    // is chosen.
    val multiCoreWorkerOffers = Seq(new WorkerOffer("executor0", "host0", taskCpus),
      new WorkerOffer("executor1", "host1", numFreeCores))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(1 === taskDescriptions.length)
    assert("executor0" === taskDescriptions(0).executorId)
  }

  test("Scheduler does not crash when tasks are not serializable") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskCpus = 2

    sc.conf.set("spark.task.cpus", taskCpus.toString)
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    val dagScheduler = new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }
    val numFreeCores = 1
    taskScheduler.setDAGScheduler(dagScheduler)
    val taskSet = new TaskSet(
      Array(new NotSerializableFakeTask(1, 0), new NotSerializableFakeTask(0, 1)), 0, 0, 0, null)
    val multiCoreWorkerOffers = Seq(new WorkerOffer("executor0", "host0", taskCpus),
      new WorkerOffer("executor1", "host1", numFreeCores))
    taskScheduler.submitTasks(taskSet)
    var taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // Now check that we can still submit tasks
    // Even if one of the tasks has not-serializable tasks, the other task set should
    // still be processed without error
    taskScheduler.submitTasks(taskSet)
    taskScheduler.submitTasks(FakeTask.createTaskSet(1))
    taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(taskDescriptions.map(_.executorId) === Seq("executor0"))
  }

  test("refuse to schedule concurrent attempts for the same stage (SPARK-8103)") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    val dagScheduler = new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }
    taskScheduler.setDAGScheduler(dagScheduler)
    val attempt1 = FakeTask.createTaskSet(1, 0, 0)
    val attempt2 = FakeTask.createTaskSet(1, 0, 1)
    taskScheduler.submitTasks(attempt1)
    intercept[IllegalStateException] { taskScheduler.submitTasks(attempt2) }

    // OK to submit multiple if previous attempts are all zombie
    taskScheduler.taskSetManagerForAttempt(attempt1.stageId, attempt1.stageAttemptId)
      .get.isZombie = true
    taskScheduler.submitTasks(attempt2)
    val attempt3 = FakeTask.createTaskSet(1, 0, 2)
    intercept[IllegalStateException] { taskScheduler.submitTasks(attempt3) }
    taskScheduler.taskSetManagerForAttempt(attempt2.stageId, attempt2.stageAttemptId)
      .get.isZombie = true
    taskScheduler.submitTasks(attempt3)
  }

  test("don't schedule more tasks after a taskset is zombie") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }

    val numFreeCores = 1
    val workerOffers = Seq(new WorkerOffer("executor0", "host0", numFreeCores))
    val attempt1 = FakeTask.createTaskSet(10)

    // submit attempt 1, offer some resources, some tasks get scheduled
    taskScheduler.submitTasks(attempt1)
    val taskDescriptions = taskScheduler.resourceOffers(workerOffers).flatten
    assert(1 === taskDescriptions.length)

    // now mark attempt 1 as a zombie
    taskScheduler.taskSetManagerForAttempt(attempt1.stageId, attempt1.stageAttemptId)
      .get.isZombie = true

    // don't schedule anything on another resource offer
    val taskDescriptions2 = taskScheduler.resourceOffers(workerOffers).flatten
    assert(0 === taskDescriptions2.length)

    // if we schedule another attempt for the same stage, it should get scheduled
    val attempt2 = FakeTask.createTaskSet(10, 0, 1)

    // submit attempt 2, offer some resources, some tasks get scheduled
    taskScheduler.submitTasks(attempt2)
    val taskDescriptions3 = taskScheduler.resourceOffers(workerOffers).flatten
    assert(1 === taskDescriptions3.length)
    val mgr = taskScheduler.taskIdToTaskSetManager.get(taskDescriptions3(0).taskId).get
    assert(mgr.taskSet.stageAttemptId === 1)
  }

  test("if a zombie attempt finishes, continue scheduling tasks for non-zombie attempts") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }

    val numFreeCores = 10
    val workerOffers = Seq(new WorkerOffer("executor0", "host0", numFreeCores))
    val attempt1 = FakeTask.createTaskSet(10)

    // submit attempt 1, offer some resources, some tasks get scheduled
    taskScheduler.submitTasks(attempt1)
    val taskDescriptions = taskScheduler.resourceOffers(workerOffers).flatten
    assert(10 === taskDescriptions.length)

    // now mark attempt 1 as a zombie
    val mgr1 = taskScheduler.taskSetManagerForAttempt(attempt1.stageId, attempt1.stageAttemptId).get
    mgr1.isZombie = true

    // don't schedule anything on another resource offer
    val taskDescriptions2 = taskScheduler.resourceOffers(workerOffers).flatten
    assert(0 === taskDescriptions2.length)

    // submit attempt 2
    val attempt2 = FakeTask.createTaskSet(10, 0, 1)
    taskScheduler.submitTasks(attempt2)

    // attempt 1 finished (this can happen even if it was marked zombie earlier -- all tasks were
    // already submitted, and then they finish)
    taskScheduler.taskSetFinished(mgr1, true)

    // now with another resource offer, we should still schedule all the tasks in attempt2
    val taskDescriptions3 = taskScheduler.resourceOffers(workerOffers).flatten
    assert(10 === taskDescriptions3.length)

    taskDescriptions3.foreach { task =>
      val mgr = taskScheduler.taskIdToTaskSetManager.get(task.taskId).get
      assert(mgr.taskSet.stageAttemptId === 1)
    }
  }

  test("tasks are not re-scheduled while executor loss reason is pending") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }

    val e0Offers = Seq(new WorkerOffer("executor0", "host0", 1))
    val e1Offers = Seq(new WorkerOffer("executor1", "host0", 1))
    val attempt1 = FakeTask.createTaskSet(1)

    // submit attempt 1, offer resources, task gets scheduled
    taskScheduler.submitTasks(attempt1)
    val taskDescriptions = taskScheduler.resourceOffers(e0Offers).flatten
    assert(1 === taskDescriptions.length)

    // mark executor0 as dead but pending fail reason
    taskScheduler.executorLost("executor0", LossReasonPending)

    // offer some more resources on a different executor, nothing should change
    val taskDescriptions2 = taskScheduler.resourceOffers(e1Offers).flatten
    assert(0 === taskDescriptions2.length)

    // provide the actual loss reason for executor0
    taskScheduler.executorLost("executor0", SlaveLost("oops"))

    // executor0's tasks should have failed now that the loss reason is known, so offering more
    // resources should make them be scheduled on the new executor.
    val taskDescriptions3 = taskScheduler.resourceOffers(e1Offers).flatten
    assert(1 === taskDescriptions3.length)
    assert("executor1" === taskDescriptions3(0).executorId)
  }

  test("scheduled tasks obey task and stage blacklists") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val blacklist = mock[BlacklistTracker]
    val taskScheduler = new TaskSchedulerImpl(sc, 4, blacklist)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }
    val stage0 = FakeTask.createTaskSet(numTasks = 2, stageId = 0, stageAttemptId = 0)
    val stage1 = FakeTask.createTaskSet(numTasks = 2, stageId = 1, stageAttemptId = 0)
    val stage2 = FakeTask.createTaskSet(numTasks = 2, stageId = 2, stageAttemptId = 0)
    taskScheduler.submitTasks(stage0)
    taskScheduler.submitTasks(stage1)
    taskScheduler.submitTasks(stage2)

    val offers = Seq(
      new WorkerOffer("executor0", "host0", 1),
      new WorkerOffer("executor1", "host1", 1),
      new WorkerOffer("executor2", "host1", 1),
      new WorkerOffer("executor3", "host3", 10)
    )

    when(blacklist.nodeBlacklist()).thenReturn(Set[String]())
    when(blacklist.nodeBlacklistForStage(0)).thenReturn(Set("host1"))
    when(blacklist.nodeBlacklistForStage(1)).thenReturn(Set[String]())
    when(blacklist.nodeBlacklistForStage(2)).thenReturn(Set[String]())
    when(blacklist.isExecutorBlacklisted(anyInt(), anyString())).thenAnswer( new Answer[Boolean]{
      override def answer(invocation: InvocationOnMock): Boolean = {
        val args = invocation.getArguments()
        val stageId = args(0).asInstanceOf[Int]
        val execId = args(1).asInstanceOf[String]
        stageId == 1 && execId == "executor3"
      }
    })

    when(blacklist.isExecutorBlacklisted(anyString(), anyInt(), anyInt()))
      .thenAnswer( new Answer[Boolean]() {
        override def answer(invocation: InvocationOnMock): Boolean = {
          val args = invocation.getArguments
          val exec = args(0).asInstanceOf[String]
          val stage = args(1).asInstanceOf[Int]
          val part = args(2).asInstanceOf[Int]
          exec == "executor0" && stage == 0 && part == 0
        }
      })

    (0 to 2).foreach { stageId =>
      taskScheduler.taskSetManagerForAttempt(stageId, 0).get.setBlacklistTracker(blacklist)
    }

    val tasks0 = taskScheduler.resourceOffers(offers).flatten
    // these verifications are tricky b/c we reference them multiple times -- also invoked when we
    // check if we need to abort any stages from unschedulability.
    verify(blacklist, atLeast(1)).nodeBlacklist()
    verify(blacklist, atLeast(1)).nodeBlacklistForStage(0)
    verify(blacklist, atLeast(1)).nodeBlacklistForStage(1)
    verify(blacklist, atLeast(1)).nodeBlacklistForStage(2)
    for {
      exec <- Seq("executor1", "executor2")
      part <- 0 to 1
    } {
      // the node blacklist should ensure we never check the task blacklist.  This is important
      // for performance, otherwise we end up changing an O(1) operation into a
      // O(numPendingTasks) one
      verify(blacklist, never).isExecutorBlacklisted(exec, 0, part)
    }

    // similarly, the executor blacklist for an entire stage should prevent us from ever checking
    // the blacklist for specific parts in a stage.
    (0 to 1).foreach { part =>
      verify(blacklist, never).isExecutorBlacklisted("executor3", 1, part)
    }

    // we should schedule all tasks.
    assert(tasks0.size == 6)
    def tasksForStage(stageId: Int): Seq[TaskDescription] = {
      tasks0.filter{_.name.contains(s"stage $stageId")}
    }
    tasksForStage(0).foreach { task =>
      // exec 1 & 2 blacklisted for node
      // exec 0 blacklisted just for part 0
      if (task.index == 0) {
        assert(task.executorId == "executor3")
      } else {
        assert(Set("executor0", "executor3").contains(task.executorId))
      }
    }
    tasksForStage(1).foreach { task =>
      // exec 3 blacklisted
      assert("executor3" != task.executorId)
    }
    // no restrictions on stage 2

    // have all tasksets finish (stages 0 & 1 successfully, 2 unsuccessfully)
    (0 to 2).foreach { stageId =>
      val tasks = tasksForStage(stageId)
      val tsm = taskScheduler.taskSetManagerForAttempt(stageId, 0).get
      val valueSer = SparkEnv.get.serializer.newInstance()
      if (stageId == 2) {
        // just need to make one task fail 4 times
        var task = tasks(0)
        val taskIndex = task.index
        (0 until 4).foreach { attempt =>
          assert(task.attemptNumber == attempt)
          tsm.handleFailedTask(task.taskId, TaskState.FAILED, TaskResultLost)
          val nextAttempts = taskScheduler.resourceOffers(Seq(WorkerOffer("4", "host4", 1))).flatten
          if (attempt < 3) {
            assert(nextAttempts.size == 1)
            task = nextAttempts(0)
            assert(task.index == taskIndex)
          } else {
            assert(nextAttempts.size == 0)
          }
        }
        // end the other task of the taskset, doesn't matter whether it succeeds or fails
        val otherTask = tasks(1)
        val result = new DirectTaskResult[Int](valueSer.serialize(otherTask.taskId), Seq())
        tsm.handleSuccessfulTask(otherTask.taskId, result)
      } else {
        tasks.foreach { task =>
          val result = new DirectTaskResult[Int](valueSer.serialize(task.taskId), Seq())
          tsm.handleSuccessfulTask(task.taskId, result)
        }
      }
    }

    // the tasksSets complete, so the tracker should be notified
    verify(blacklist, times(1)).taskSetSucceeded(0)
    verify(blacklist, times(1)).taskSetSucceeded(1)
    verify(blacklist, times(1)).taskSetFailed(2)
  }

  test("scheduled tasks obey node and executor blacklists") {
    // TODO another case with full node & executor blacklist, which should prevent *everything*
    // else from being called.
    pending
  }

}
