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
package org.apache.spark.network.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

public interface LargeByteBuffer {
    public long capacity();

    public byte get();

    public void get(byte[] dst,int offset, int length);

    public void position(long position);

    public long position();

    /** doesn't copy data, just copies references & offsets */
    public LargeByteBuffer duplicate();

    public void put(LargeByteBuffer bytes);

    public long remaining();

    //TODO checks on limit semantics

    /**
     * Sets this buffer's limit. If the position is larger than the new limit then it is set to the
     * new limit. If the mark is defined and larger than the new limit then it is discarded.
     */
    public void limit(long newLimit);

    /**
     * return this buffer's limit
     * @return
     */
    public long limit();

    // The semantics are currently vague -- are you guaranteed to have these
    // buffers backed by the same data?  What if there are multiple buffers here, but the total
    // size is less than 2g?  Primary use case is Serializer.deserialize() -- need to look
    // at call sites more closely
    /**
     * Get the data of this buffer in one ByteBuffer.  Implementations may throw an exception
     * if that is not possible.
     * @return
     */
    public ByteBuffer asByteBuffer();

    //List b/c we need to know the size.  Could also use Iterator w/ separate numBuffers method
    /**
     * A view over all the data, broken into individual ByteBuffers.  Note that position & limit
     * of this buffer are in no way reflected in the underlying buffers.
     * @return
     */
    public List<ByteBuffer> nioBuffers();

}
