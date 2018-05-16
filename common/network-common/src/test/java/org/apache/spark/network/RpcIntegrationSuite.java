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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.server.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class RpcIntegrationSuite {
  static TransportConf conf;
  static TransportServer server;
  static TransportClientFactory clientFactory;
  static RpcHandler rpcHandler;
  static List<String> oneWayMsgs;
  static StreamTestHelper testData;

  static ConcurrentHashMap<String, VerifyingStreamCallback> streamCallbacks =
      new ConcurrentHashMap<>();

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);
    testData = new StreamTestHelper();
    rpcHandler = new RpcHandler() {
      @Override
      public void receive(
          TransportClient client,
          ByteBuffer message,
          StreamData streamData,
          RpcResponseCallback callback) {
        String msg = JavaUtils.bytesToString(message);
        if (streamData != null) {
          receiveStream(msg, streamData);
        } else {
          String[] parts = msg.split("/");
          if (parts[0].equals("hello")) {
            callback.onSuccess(JavaUtils.stringToBytes("Hello, " + parts[1] + "!"));
          } else if (parts[0].equals("return error")) {
            callback.onFailure(new RuntimeException("Returned: " + parts[1]));
          } else if (parts[0].equals("throw error")) {
            throw new RuntimeException("Thrown: " + parts[1]);
          }
        }
      }

      @Override
      public void receive(TransportClient client, ByteBuffer message) {
        oneWayMsgs.add(JavaUtils.bytesToString(message));
      }

      @Override
      public StreamManager getStreamManager() { return new OneForOneStreamManager(); }
    };
    TransportContext context = new TransportContext(conf, rpcHandler);
    server = context.createServer();
    clientFactory = context.createClientFactory();
    oneWayMsgs = new ArrayList<>();
  }

  private static void receiveStream(String msg, StreamData streamData) {
    try {
      if (msg.startsWith("fail/")) {
        String[] parts = msg.split("/");
        switch(parts[1]) {
          case "no callback":
            // don't register anything here, check the rpc error response is appropriate
            break;
          case "exception":
            StreamCallback callback = new StreamCallback() {
              @Override
              public void onData(String streamId, ByteBuffer buf) throws IOException {
                throw new IOException("failed to read stream data!");
              }

              @Override
              public void onComplete(String streamId) throws IOException {
              }

              @Override
              public void onFailure(String streamId, Throwable cause) throws IOException {
              }
            };
            streamData.registerStreamCallback(msg, callback);
            break;
          case "multiple":
            VerifyingStreamCallback streamCallback = new VerifyingStreamCallback(msg);
            streamData.registerStreamCallback(msg, streamCallback);
            streamData.registerStreamCallback(msg, streamCallback);
            break;
        }
      } else {
        VerifyingStreamCallback streamCallback = new VerifyingStreamCallback(msg);
        streamData.registerStreamCallback(msg, streamCallback);
        streamCallbacks.put(msg, streamCallback);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void tearDown() {
    server.close();
    clientFactory.close();
    testData.cleanup();
  }

  static class RpcResult {
    public Set<String> successMessages;
    public Set<String> errorMessages;
  }

  private RpcResult sendRPC(String ... commands) throws Exception {
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    final Semaphore sem = new Semaphore(0);

    final RpcResult res = new RpcResult();
    res.successMessages = Collections.synchronizedSet(new HashSet<String>());
    res.errorMessages = Collections.synchronizedSet(new HashSet<String>());

    RpcResponseCallback callback = new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer message) {
        String response = JavaUtils.bytesToString(message);
        res.successMessages.add(response);
        sem.release();
      }

      @Override
      public void onFailure(Throwable e) {
        res.errorMessages.add(e.getMessage());
        sem.release();
      }
    };

    for (String command : commands) {
      client.sendRpc(JavaUtils.stringToBytes(command), callback);
    }

    if (!sem.tryAcquire(commands.length, 5, TimeUnit.SECONDS)) {
      fail("Timeout getting response from the server");
    }
    client.close();
    return res;
  }

  private RpcResult sendRpcWithStream(String... streams) throws Exception {
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    final Semaphore sem = new Semaphore(0);
    RpcResult res = new RpcResult();
    res.successMessages = Collections.synchronizedSet(new HashSet<String>());
    res.errorMessages = Collections.synchronizedSet(new HashSet<String>());
    RpcResponseCallback callback = new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer message) {
        String response = JavaUtils.bytesToString(message);
        res.successMessages.add(response);
        sem.release();
      }

      @Override
      public void onFailure(Throwable e) {
        res.errorMessages.add(e.getMessage());
        sem.release();
      }
    };

    for (String stream: streams) {
      int idx = stream.lastIndexOf('/');
      ManagedBuffer meta = new NioManagedBuffer(JavaUtils.stringToBytes(stream));
      String streamName = (idx == -1) ? stream : stream.substring(idx + 1);
      ManagedBuffer data = testData.openStream(conf, streamName);
      client.uploadStream(meta, data, callback);
    }
    streamCallbacks.values().forEach(streamCallback -> {
      try {
        streamCallback.waitForCompletionAndVerify(TimeUnit.SECONDS.toMillis(5));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });


    if (!sem.tryAcquire(streams.length, 5, TimeUnit.SECONDS)) {
      fail("Timeout getting response from the server");
    }
    client.close();
    return res;
  }

  @Test
  public void singleRPC() throws Exception {
    RpcResult res = sendRPC("hello/Aaron");
    assertEquals(res.successMessages, Sets.newHashSet("Hello, Aaron!"));
    assertTrue(res.errorMessages.isEmpty());
  }

  @Test
  public void doubleRPC() throws Exception {
    RpcResult res = sendRPC("hello/Aaron", "hello/Reynold");
    assertEquals(res.successMessages, Sets.newHashSet("Hello, Aaron!", "Hello, Reynold!"));
    assertTrue(res.errorMessages.isEmpty());
  }

  @Test
  public void returnErrorRPC() throws Exception {
    RpcResult res = sendRPC("return error/OK");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Returned: OK"));
  }

  @Test
  public void throwErrorRPC() throws Exception {
    RpcResult res = sendRPC("throw error/uh-oh");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Thrown: uh-oh"));
  }

  @Test
  public void doubleTrouble() throws Exception {
    RpcResult res = sendRPC("return error/OK", "throw error/uh-oh");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Returned: OK", "Thrown: uh-oh"));
  }

  @Test
  public void sendSuccessAndFailure() throws Exception {
    RpcResult res = sendRPC("hello/Bob", "throw error/the", "hello/Builder", "return error/!");
    assertEquals(res.successMessages, Sets.newHashSet("Hello, Bob!", "Hello, Builder!"));
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Thrown: the", "Returned: !"));
  }

  @Test
  public void sendOneWayMessage() throws Exception {
    final String message = "no reply";
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    try {
      client.send(JavaUtils.stringToBytes(message));
      assertEquals(0, client.getHandler().numOutstandingRequests());

      // Make sure the message arrives.
      long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
      while (System.nanoTime() < deadline && oneWayMsgs.size() == 0) {
        TimeUnit.MILLISECONDS.sleep(10);
      }

      assertEquals(1, oneWayMsgs.size());
      assertEquals(message, oneWayMsgs.get(0));
    } finally {
      client.close();
    }
  }

  @Test
  public void sendRpcWithStreamOneAtATime() throws Exception {
    for (String stream: StreamTestHelper.STREAMS) {
      RpcResult res = sendRpcWithStream(stream);
      assertTrue("there were error messages!" + res.errorMessages, res.errorMessages.isEmpty());
      assertEquals(Sets.newHashSet(stream), res.successMessages);
    }
  }

  @Test
  public void sendRpcWithStreamConcurrently() throws Exception {
    String[] streams = new String[10];
    for (int i = 0; i < 10; i++) {
      streams[i] = StreamTestHelper.STREAMS[i % StreamTestHelper.STREAMS.length];
    }
    RpcResult res = sendRpcWithStream(streams);
    assertEquals(res.successMessages, Sets.newHashSet(StreamTestHelper.STREAMS));
    assertTrue(res.errorMessages.isEmpty());
  }

  @Test
  public void sendRpcWithStreamFailures() throws Exception {
    // when there is a failure reading stream data, we don't try to keep the channel usable,
    // just send back a decent error msg.
    RpcResult noCallbackResult = sendRpcWithStream("fail/no callback/smallBuffer", "smallBuffer");
    assertTrue("unexpected success: " + noCallbackResult.successMessages,
        noCallbackResult.successMessages.isEmpty());
    assertErrorsContain(noCallbackResult.errorMessages,
        Sets.newHashSet("Destination did not register stream handler", "closed"));

    RpcResult multiCallbackResult = sendRpcWithStream("fail/multiple/smallBuffer", "smallBuffer");
    assertTrue("unexpected success: " + multiCallbackResult.successMessages,
        multiCallbackResult.successMessages.isEmpty());
    assertErrorsContain(multiCallbackResult.errorMessages,
        Sets.newHashSet("Cannot register more than one stream callback", "closed"));

    RpcResult exceptionInCallbackResult = sendRpcWithStream("fail/exception/file", "smallBuffer");
    assertTrue("unexpected success: " + exceptionInCallbackResult.successMessages,
        exceptionInCallbackResult.successMessages.isEmpty());
    assertErrorsContain(exceptionInCallbackResult.errorMessages,
        Sets.newHashSet("Destination failed while reading stream", "Connection reset"));
  }

  private void assertErrorsContain(Set<String> errors, Set<String> contains) {
    assertEquals("Expected " + contains.size() + " errors, got " + errors.size() + "errors: " +
        errors, contains.size(), errors.size());

    Set<String> remainingErrors = Sets.newHashSet(errors);
    for (String contain : contains) {
      Iterator<String> it = remainingErrors.iterator();
      boolean foundMatch = false;
      while (it.hasNext()) {
        if (it.next().contains(contain)) {
          it.remove();
          foundMatch = true;
          break;
        }
      }
      assertTrue("Could not find error containing " + contain + "; errors: " + errors, foundMatch);
    }

    assertTrue(remainingErrors.isEmpty());
  }

  private static class VerifyingStreamCallback implements StreamCallback {
    final String streamId;
    final StreamSuite.TestCallback helper;
    final OutputStream out;
    final File outFile;
    VerifyingStreamCallback(String streamId) throws IOException {
      if (streamId.equals("file")) {
        outFile = File.createTempFile("data", ".tmp", testData.tempDir);
        out = new FileOutputStream(outFile);
      } else {
        out = new ByteArrayOutputStream();
        outFile = null;
      }
      this.streamId = streamId;
      helper = new StreamSuite.TestCallback(out);
    }

    void waitForCompletionAndVerify(long timeoutMs) throws IOException {
      helper.waitForCompletion(timeoutMs);
      if (streamId.equals("file")) {
        assertTrue("File stream did not match.", Files.equal(testData.testFile, outFile));
      } else {
        byte[] result = ((ByteArrayOutputStream)out).toByteArray();
        ByteBuffer srcBuffer = testData.srcBuffer(streamId);
        ByteBuffer base;
        synchronized (srcBuffer) {
          base = srcBuffer.duplicate();
        }
        byte[] expected = new byte[base.remaining()];
        base.get(expected);
        assertEquals(expected.length, result.length);
        assertTrue("buffers don't match", Arrays.equals(expected, result));

      }
    }

    @Override
    public void onData(String streamId, ByteBuffer buf) throws IOException {
      helper.onData(streamId, buf);
    }

    @Override
    public void onComplete(String streamId) throws IOException {
      helper.onComplete(streamId);
    }

    @Override
    public void onFailure(String streamId, Throwable cause) throws IOException {
      helper.onFailure(streamId, cause);
    }
  }
}
