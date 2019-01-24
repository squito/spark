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
package org.apache.spark.shuffle.sort;

import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.ShuffleMapOutputChannelWriter;
import org.apache.spark.shuffle.api.ShufflePartitionChannelWriter;
import org.apache.spark.util.Utils;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Writes shuffle output from one shuffle map task to the local file system, with a separate
 * index & data file.
 *
 * This corresponds to the only shuffle storage in spark <= 2.4.
 */
public class LocalShuffleMapOutputWriter implements ShuffleMapOutputChannelWriter {

  final IndexShuffleBlockResolver resolver;
  final int shuffleId;
  final int mapId;

  final File dataTmpFile;

  FileChannel dataTmpFileOutputChannel = null;
  FileOutputStream dataTmpFileOutputStream = null;

  public LocalShuffleMapOutputWriter(IndexShuffleBlockResolver resolver, int shuffleId, int mapId) {
    this.resolver = resolver;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.dataTmpFile = Utils.tempFileWith(resolver.getDataFile(shuffleId, mapId));
  }

  @Override
  public ShufflePartitionChannelWriter newPartitionWriter(int partitionId) {
    return new LocalShufflePartitionWriter();
  }

  @Override
  public void commitAllPartitions(long[] partitionLengths) {
    // this will delete the temporary index file itself if there is a failure
    resolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, dataTmpFile);
  }

  @Override
  public void abort(Exception exception) throws IOException {
    JavaUtils.closeQuietly(dataTmpFileOutputChannel);
    JavaUtils.closeQuietly(dataTmpFileOutputStream);
    if (dataTmpFile.exists() && !dataTmpFile.delete()) {
      throw new IOException("failed to delete temporary shuffle output file " + dataTmpFile);
    }
  }

  private class LocalShufflePartitionWriter implements ShufflePartitionChannelWriter {

    @Override
    public OutputStream openPartitionStream() throws FileNotFoundException {
      if (dataTmpFileOutputStream == null) {
        dataTmpFileOutputStream = new FileOutputStream(dataTmpFile);
      }
      return dataTmpFileOutputStream;
    }

    @Override
    public WritableByteChannel openPartitionChannel() throws FileNotFoundException {
      if (dataTmpFileOutputChannel == null) {
        // This file needs to opened in append mode in order to work around a Linux kernel bug that
        // affects transferTo; see SPARK-3948 for more details.
        dataTmpFileOutputChannel = new FileOutputStream(dataTmpFile, true).getChannel();
      }
      return dataTmpFileOutputChannel;
    }

    @Override
    public long commitAndGetTotalLength() {
      // do NOT close any resources here, as we'll reuse them for other outputs
      return 0;
    }

    @Override
    public void abort(Exception failureReason) {
      // don't need to to anything, it'll get handled by the wrapping LocalShuffleMapOutput
    }
  }
}
