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

package org.apache.spark.shuffle.api;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.WritableByteChannel;

/**
 * Responsible for appending bytes to a partition via passing in input streams incrementally.  This
 * corresponds to one (map, reduce) pair of partitions.
 */
public interface ShufflePartitionWriter {

  /**
   * Return a stream that should persist the bytes for this partition.
   *
   * Note that implementations are free to return the same output stream for all
   * ShufflePartitionWriters of one ShuffleMapOutputWriter.
   */
  OutputStream openPartitionStream() throws IOException;

  /**
   * Indicate that the partition was written successfully and there are no more incoming bytes. Returns
   * the length of the partition that is written. Note that returning the length is mainly for backwards
   * compatibility and should be removed in a more polished variant. After this method is called, the writer
   * will be discarded; it's expected that the implementation will close any underlying resources
   * (unless the output stream / channel is reused for the entire ShuffleMapOutputWriter).
   *
   * TODO: Not sure this needs to return a length -- maybe should be called close()?
   */
  long commitAndGetTotalLength();

  /**
   * Indicate that the write has failed for some reason and the implementation can handle the
   * failure reason. After this method is called, this writer will be discarded; it's expected that
   * the implementation will close any underlying resources.
   */
  void abort(Exception failureReason);
}
