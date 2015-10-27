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

import java.io.{FileOutputStream, FileInputStream, File}

import org.apache.spark.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerInstance

/**
 * Ensures that on each executor, there are no conflicting writes to the same shuffle files.  It
 * implements "first write wins", by atomically moving all shuffle files into their final location,
 * only if the files did not already exist. See SPARK-8029
 */
private[spark] object ShuffleOutputCoordinator extends Logging {

  /**
   * If any of the destination files do not exist, then move all of the temporary files to their
   * destinations, and return (true, the given MapStatus).  If all destination files exist, then
   * delete all temporary files, and return (false, the MapStatus from previously committed shuffle
   * output).

   * @param shuffleId
   * @param partitionId
   * @param tmpToDest  Seq of (temporary, destination) file pairs
   * @param mapStatus the [[MapStatus]] for the output already written to the the temporary files
   * @param mapStatusFile canonical location to store the mapStatus, for future attempts.  See
   *                      [[org.apache.spark.storage.ShuffleMapStatusBlockId]]
   * @param serializer to serialize the mapStatus to disk
   * @return pair of (true iff the set of temporary files was moved to the destination, the
   *         MapStatus of the winn
   *
   */
  def commitOutputs(
      shuffleId: Int,
      partitionId: Int,
      tmpToDest: Seq[(File, File)],
      mapStatus: MapStatus,
      mapStatusFile: File,
      serializer: SerializerInstance): (Boolean, MapStatus) = synchronized {
    tmpToDest.foreach { case (tmp, _) => require(tmp.exists(), s"Cannot commit non-existent " +
      s"shuffle output $tmp -- must be at least a zero-length file.")}
    val destAlreadyExists = tmpToDest.forall{_._2.exists()} && mapStatusFile.exists()
    if (!destAlreadyExists) {
      tmpToDest.foreach { case (tmp, dest) =>
        // If *some* of the destination files exist, but not all of them, then its not clear
        // what to do.  There could be a task already reading from this dest file when we delete
        // it -- but then again, something in that taskset would be doomed to fail in any case when
        // it got to the missing files.  Better to just put consistent output into place
        if (dest.exists()) {
          dest.delete()
        }
        tmp.renameTo(dest)
      }
      val out = serializer.serializeStream(new FileOutputStream(mapStatusFile))
      out.writeObject(mapStatus)
      out.close()
      (true, mapStatus)
    } else {
      logInfo(s"shuffle output for shuffle $shuffleId, partition $partitionId already exists, " +
        s"not overwriting.  Another task must have created this shuffle output.")
      tmpToDest.foreach{ case (tmp, _) => tmp.delete()}
      val in = serializer.deserializeStream(new FileInputStream(mapStatusFile))
      val readStatus = in.readObject[MapStatus]
      in.close()
      (false, readStatus)
    }
  }
}
