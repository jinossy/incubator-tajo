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

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.TUtil;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientBossPool;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.ThreadNameDeterminer;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class RpcConnectionPool {
  private static final Log LOG = LogFactory.getLog(RpcConnectionPool.class);

  private Map<RpcConnectionKey, NettyClientBase> connections = TUtil.newConcurrentHashMap();

  private static RpcConnectionPool instance;

  private TajoConf conf;
  private ChannelGroup accepted = new DefaultChannelGroup();
  //netty default value
  protected static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;
  protected static int nettyWorkerCount;

  /**
   * make this factory static thus all clients can share its thread pool.
   * NioClientSocketChannelFactory has only one method newChannel() visible for user, which is thread-safe
   */
  private static final ClientSocketChannelFactory factory;
  private static final NioClientBossPool bossPool;
  private static final NioWorkerPool workerPool;

  static {
    TajoConf conf = new TajoConf();

    nettyWorkerCount = conf.getIntVar(TajoConf.ConfVars.RPC_CLIENT_SOCKET_IO_THREADS);
    if (nettyWorkerCount <= 0) {
      nettyWorkerCount = DEFAULT_IO_THREADS;
    }

    ThreadFactory bossFactory = new ThreadFactoryBuilder()
        .setNameFormat("RpcConnectionPool Boss #%d")
        .build();
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
        .setNameFormat("RpcConnectionPool Worker #%d")
        .build();

    //shared woker and boss poll
    bossPool = new NioClientBossPool(Executors.newCachedThreadPool(bossFactory), 1,
        new HashedWheelTimer(), ThreadNameDeterminer.CURRENT);
    workerPool = new NioWorkerPool(Executors.newCachedThreadPool(workerFactory), nettyWorkerCount,
        ThreadNameDeterminer.CURRENT);

    factory = new NioClientSocketChannelFactory(bossPool, workerPool);
  }

  public static ClientSocketChannelFactory getChannelFactory(){
    return factory;
  }

  private RpcConnectionPool(TajoConf conf) {
    this.conf = conf;
  }

  public synchronized static RpcConnectionPool getPool(TajoConf conf) {
    if(instance == null) {
      instance = new RpcConnectionPool(conf);
    }

    return instance;
  }

  private NettyClientBase makeConnection(RpcConnectionKey rpcConnectionKey) throws Exception {
    NettyClientBase client;
    if(rpcConnectionKey.asyncMode) {
      client = new AsyncRpcClient(rpcConnectionKey.protocolClass, rpcConnectionKey.addr, factory);
    } else {
      client = new BlockingRpcClient(rpcConnectionKey.protocolClass, rpcConnectionKey.addr, factory);
    }
    accepted.add(client.getChannel());
    return client;
  }

  public NettyClientBase getConnection(InetSocketAddress addr,
                                       Class protocolClass, boolean asyncMode) throws Exception {
    RpcConnectionKey key = new RpcConnectionKey(addr, protocolClass, asyncMode);
    NettyClientBase client;

    synchronized (connections) {
      if (!connections.containsKey(key)) {
        connections.put(key, makeConnection(key));
      }
      client = connections.get(key);
    }

    if (!client.getChannel().isOpen() || !client.getChannel().isConnected()) {
      LOG.warn("Try to reconnect : " + client.getChannel().getRemoteAddress());
      client.reconnect();
    }
    return client;
  }

  public void releaseConnection(NettyClientBase client) {
    if (client == null) return;

    try {
      if(!client.getChannel().isOpen()){
//        synchronized(connections) {
//          connections.remove(client.getKey());
//        }
//        client.close();
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Current Connections [" + connections.size() + "] Accepted: " + accepted.size());

      }
    } catch (Exception e) {
      LOG.error("Can't close connection:" + client.getKey() + ":" + e.getMessage(), e);
    }
  }

  public void closeConnection(NettyClientBase client) {
    if (client == null) {
      return;
    }

    try {
      if(LOG.isDebugEnabled()) {
        LOG.debug("CloseConnection [" + client.getKey() + "]");
      }
      synchronized(connections) {
        connections.remove(client.getKey());
      }
      client.close();
    } catch (Exception e) {
      LOG.error("Can't close connection:" + client.getKey() + ":" + e.getMessage(), e);
    }
  }

  public synchronized void close() {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Pool Closed");
    }
    synchronized(connections) {
      for(NettyClientBase eachClient: connections.values()) {
        try {
          eachClient.close();
        } catch (Exception e) {
          LOG.error("close client pool error", e);
        }
      }
      accepted.close().awaitUninterruptibly();
      connections.clear();
    }
  }

  public synchronized void shutdown(){
    factory.releaseExternalResources();
  }

  static class RpcConnectionKey {
    final InetSocketAddress addr;
    final Class protocolClass;
    final boolean asyncMode;

    public RpcConnectionKey(InetSocketAddress addr,
                            Class protocolClass, boolean asyncMode) {
      this.addr = addr;
      this.protocolClass = protocolClass;
      this.asyncMode = asyncMode;
    }

    @Override
    public String toString() {
      return "["+ protocolClass + "] " + addr + "," + asyncMode;
    }

    @Override
    public boolean equals(Object obj) {
      if(!(obj instanceof RpcConnectionKey)) {
        return false;
      }

      return toString().equals(obj.toString());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(protocolClass, addr, asyncMode);
    }
  }
}
