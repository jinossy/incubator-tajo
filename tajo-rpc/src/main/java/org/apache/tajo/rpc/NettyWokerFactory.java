/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.rpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.conf.TajoConf;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.ThreadNameDeterminer;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class NettyWokerFactory {
  private static final Log LOG = LogFactory.getLog(NettyWokerFactory.class);
  private static ClientSocketChannelFactory factory;

  private NettyWokerFactory(){
  }

  /**
   * make this factory static thus all clients can share its thread pool.
   * NioClientSocketChannelFactory has only one method newChannel() visible for user, which is thread-safe
   */
  public static synchronized ClientSocketChannelFactory getSharedClientChannelFactory(){
    //shared woker and boss poll
    if(factory == null){
      TajoConf conf = new TajoConf();
      int nettyWorkerNum = conf.getIntVar(TajoConf.ConfVars.INTERNAL_RPC_CLIENT_IO_THREAD_NUM);
      factory = createClientChannelFactory("Client", nettyWorkerNum);
    }
    return factory;
  }

  // Client must release the external resources
  public static synchronized ClientSocketChannelFactory createClientChannelFactory(String name, int ioThreadNum) {
    if(LOG.isDebugEnabled()){
      LOG.debug("Create " + name + " ClientSocketChannelFactory. I/O threads:" + ioThreadNum);
    }

    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    ThreadFactory bossFactory = builder.setNameFormat(name + " Boss #%d").build();
    ThreadFactory workerFactory = builder.setNameFormat(name + " Worker #%d").build();

    NioClientBossPool bossPool = new NioClientBossPool(Executors.newCachedThreadPool(bossFactory), 1,
        new HashedWheelTimer(), ThreadNameDeterminer.CURRENT);
    NioWorkerPool workerPool = new NioWorkerPool(Executors.newCachedThreadPool(workerFactory), ioThreadNum,
        ThreadNameDeterminer.CURRENT);

    return new NioClientSocketChannelFactory(bossPool, workerPool);
  }

  // Client must release the external resources
  public static synchronized ServerSocketChannelFactory createServerChannelFactory(String name, int ioThreadNum) {
    if(LOG.isInfoEnabled()){
      LOG.info("Create " + name + " ServerSocketChannelFactory. I/O threads:" + ioThreadNum);
    }
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    ThreadFactory bossFactory = builder.setNameFormat(name + " Server Boss #%d").build();
    ThreadFactory workerFactory = builder.setNameFormat(name + " Server Worker #%d").build();

    NioServerBossPool bossPool =
        new NioServerBossPool(Executors.newCachedThreadPool(bossFactory), 1, ThreadNameDeterminer.CURRENT);
    NioWorkerPool workerPool =
        new NioWorkerPool(Executors.newCachedThreadPool(workerFactory), ioThreadNum, ThreadNameDeterminer.CURRENT);

    return new NioServerSocketChannelFactory(bossPool, workerPool);
  }

  public static synchronized void shutdown(){
    if(LOG.isDebugEnabled()) {
      LOG.debug("Shutdown Shared RPC Pool");
    }
    factory.releaseExternalResources();
    factory = null;
  }
}
