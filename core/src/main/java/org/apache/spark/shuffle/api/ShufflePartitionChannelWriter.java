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

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * An extension of ShufflePartitionWriter for shuffle storage implementations that allow transfering
 * shuffle data directly to a WritableByteChannel, rather than an OutputStream.
 *
 * Callers may choose to use either the Channel or the OutputStream, but they must not mix usage of
 * both.
 */
public interface ShufflePartitionChannelWriter extends ShufflePartitionWriter {

  /**
   * Get a channel to write data to, to allow a more efficient transfer of data.
   *
   * Note that implementations are free to return the same channel for all ShufflePartitionWriters
   * of one ShuffleMapOutputWriter.
   */
  WritableByteChannel openPartitionChannel() throws IOException;
}
