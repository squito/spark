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

package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;

/**
 * A request to start fetching a remote block as a stream, and store it locally.
 *
 * This is initiated by a node which wishes to replicate a block, and it wants the receiving end
 * to stream the block directly to disk (not store it all in memory, as it would with UploadBlock).
 *
 * The actual block data is not contained here.  It is in the streamData in the RpcHandler.receive()
 * call.
 */
public class UploadBlockStream extends BlockTransferMessage {
  public final String blockId;
  // TODO: StorageLevel is serialized separately in here because StorageLevel is not available in
  // this package. We should avoid this hack.
  public final byte[] metadata;

  /**
   * @param metadata Meta-information about block, typically StorageLevel.
   */
  public UploadBlockStream(String blockId, byte[] metadata) {
    this.blockId = blockId;
    this.metadata = metadata;
  }

  @Override
  protected Type type() { return Type.STREAM_BLOCK; }

  @Override
  public int hashCode() {
    int objectsHashCode = Objects.hashCode(blockId);
    return objectsHashCode * 41 + Arrays.hashCode(metadata);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("blockId", blockId)
      .add("metadata size", metadata.length)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof UploadBlockStream) {
      UploadBlockStream o = (UploadBlockStream) other;
      return Objects.equal(blockId, o.blockId)
        && Arrays.equals(metadata, o.metadata);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(blockId)
      + Encoders.ByteArrays.encodedLength(metadata);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, blockId);
    Encoders.ByteArrays.encode(buf, metadata);
  }

  public static UploadBlockStream decode(ByteBuf buf) {
    String blockId = Encoders.Strings.decode(buf);
    byte[] metadata = Encoders.ByteArrays.decode(buf);
    return new UploadBlockStream(blockId, metadata);
  }
}
