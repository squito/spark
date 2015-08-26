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

package org.apache.spark.network;

import org.apache.spark.network.protocol.*;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.util.TransportConf;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains the context to create a {@link org.apache.spark.network.server.TransportServer}, {@link org.apache.spark.network.client.TransportClientFactory}, and to
 * setup Netty Channel pipelines with a {@link org.apache.spark.network.server.TransportChannelHandler}.
 *
 * There are two communication protocols that the TransportClient provides, control-plane RPCs and
 * data-plane "chunk fetching". The handling of the RPCs is performed outside of the scope of the
 * TransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 *
 * The TransportServer and TransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a TransportClient, this enables server
 * processes to send messages back to the client on an existing channel.
 */
public class LargeTransportContext extends TransportContext {

  private final LargeMessageEncoder encoder = new LargeMessageEncoder();
  private final LargeMessageDecoder decoder = new LargeMessageDecoder();

  public LargeTransportContext(TransportConf conf, RpcHandler rpcHandler) {
    super(conf, rpcHandler);
  }

  @Override
  protected List<Handler> getHandlers() {
    ArrayList<Handler> r = new ArrayList<Handler>();
    r.add(new Handler("encoder", encoder));
    r.add(new Handler("frameDecoder", new FixedChunkLargeFrameDecoder(65536)));
    r.add(new Handler("decoder", decoder));
    return r;
  }

}
