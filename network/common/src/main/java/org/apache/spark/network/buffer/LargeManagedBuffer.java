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
import java.io.InputStream;
import java.util.List;

public abstract class LargeManagedBuffer {

  public abstract long size();

  public abstract InputStream createInputStream() throws IOException;

  public abstract LargeManagedBuffer retain();

  public abstract LargeManagedBuffer release();

  /**
   * Convert the buffer into Netty objects, used to write the data out.
   */
  public abstract List<? extends Object> convertToNetty() throws IOException;
}
