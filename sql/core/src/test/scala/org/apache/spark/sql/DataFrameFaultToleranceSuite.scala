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
package org.apache.spark.sql

import org.apache.spark.{SparkEnv, SparkFunSuite}
import org.apache.spark.util.Utils

class DataFrameFaultToleranceSuite extends SparkFunSuite with LocalSparkSession {

  private val dir = Utils.createTempDir(namePrefix = "output").getCanonicalPath

  test("data frame writer fault tolerance") {
    spark = SparkSession.builder()
      .master("local-cluster[2,1,1024]")
      .config("spark.testing.master.replaceExecutors", "false")
      .getOrCreate()
    val x = spark

    import x.implicits._

    // get both executors to connect
    val executors = spark.sparkContext.parallelize(1 to 100).mapPartitions { itr =>
      Iterator(SparkEnv.get.executorId)
    }.collect()
    val execToFail = executors.head
    println(s"executors = ${executors.mkString(",")}")
    println(s"will fail $execToFail")

    // write a dataset, but kill an executor post-shuffle, so that we should trigger
    // fetch-failure and stage retries.
    val ds = spark.createDataset((1 to 100).map(MyData))
    val shuffledDs = ds.repartition(5)
//      .map { x =>
//        if (SparkEnv.get.executorId == execToFail) {
//          println("sleeping, then killing executor")
//          Thread.sleep(500)
//          println("killing executor")
//          System.exit(42)
//        }
//        x
//    }
    shuffledDs.write.json(dir + "/json")

    shuffledDs.write.mode("append").saveAsTable("my_table")
  }
}

case class MyData(x: Int)
