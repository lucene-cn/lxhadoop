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
package org.apache.hadoop.hdfs.client.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.hdfs.net.DomainPeer;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.DomainSocketFactory;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache.ShortCircuitReplicaCreator;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitReplica;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitReplicaInfo;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.Slot;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.hdfs.util.IOUtilsClient;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.PerformanceAdvisory;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.List;

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitFdResponse.USE_RECEIPT_VERIFICATION;


/**
 * Utility class to create BlockReader implementations.
 */
@InterfaceAudience.Private
public class BlockReaderBatchFactory implements ShortCircuitReplicaCreator {
  static final Logger LOG = LoggerFactory.getLogger(BlockReaderBatchFactory.class);

  @Override
  public ShortCircuitReplicaInfo createShortCircuitReplicaInfo() {
    return null;
  }

  public static class FailureInjector {
    public void injectRequestFileDescriptorsFailure() throws IOException {
      // do nothing
    }
    public boolean getSupportsReceiptVerification() {
      return true;
    }
  }


  private final DfsClientConf conf;

  /**
   * Injects failures into specific operations during unit tests.
   */
  private static FailureInjector failureInjector = new FailureInjector();

  /**
   * The file name, for logging and debugging purposes.
   */
  private String fileName;

  /**
   * The block ID and block pool ID to use.
   */
  private ExtendedBlock block;

  /**
   * The block token to use for security purposes.
   */
  private Token<BlockTokenIdentifier> token;

  /**
   * The offset within the block to start reading at.
   */
  private long[] startOffset;

  /**
   * If false, we won't try to verify the block checksum.
   */
  private boolean verifyChecksum;

  /**
   * The name of this client.
   */
  private String clientName;

  /**
   * The DataNode we're talking to.
   */
  private DatanodeInfo datanode;

  /**
   * StorageType of replica on DataNode.
   */
  private StorageType storageType;

  /**
   * If false, we won't try short-circuit local reads.
   */
  private boolean allowShortCircuitLocalReads;

  /**
   * The ClientContext to use for things like the PeerCache.
   */
  private ClientContext clientContext;

  /**
   * Number of bytes to read. Must be set to a non-negative value.
   */
  private long[] length ;

  /**
   * Caching strategy to use when reading the block.
   */
  private CachingStrategy cachingStrategy;

  /**
   * Socket address to use to connect to peer.
   */
  private InetSocketAddress inetSocketAddress;

  /**
   * Remote peer factory to use to create a peer, if needed.
   */
  private RemotePeerFactory remotePeerFactory;

  /**
   * UserGroupInformation to use for legacy block reader local objects,
   * if needed.
   */
  private UserGroupInformation userGroupInformation;

  /**
   * Configuration to use for legacy block reader local objects, if needed.
   */
  private Configuration configuration;

  /**
   * The HTrace tracer to use.
   */
  private Tracer tracer;

  /**
   * Information about the domain socket path we should use to connect to the
   * local peer-- or null if we haven't examined the local domain socket.
   */
  private DomainSocketFactory.PathInfo pathInfo;

  /**
   * The remaining number of times that we'll try to pull a socket out of the
   * cache.
   */
  private int remainingCacheTries;

  public BlockReaderBatchFactory(DfsClientConf conf) {
    this.conf = conf;
    this.remainingCacheTries = conf.getNumCachedConnRetry();
  }

  public BlockReaderBatchFactory setFileName(String fileName) {
    this.fileName = fileName;
    return this;
  }

  public BlockReaderBatchFactory setBlock(ExtendedBlock block) {
    this.block = block;
    return this;
  }

  public BlockReaderBatchFactory setBlockToken(Token<BlockTokenIdentifier> token) {
    this.token = token;
    return this;
  }

  public BlockReaderBatchFactory setStartOffset(long[] startOffset) {
    this.startOffset = startOffset;
    return this;
  }

  public BlockReaderBatchFactory setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
    return this;
  }

  public BlockReaderBatchFactory setClientName(String clientName) {
    this.clientName = clientName;
    return this;
  }

  public BlockReaderBatchFactory setDatanodeInfo(DatanodeInfo datanode) {
    this.datanode = datanode;
    return this;
  }

  public BlockReaderBatchFactory setStorageType(StorageType storageType) {
    this.storageType = storageType;
    return this;
  }

  public BlockReaderBatchFactory setAllowShortCircuitLocalReads(
      boolean allowShortCircuitLocalReads) {
    this.allowShortCircuitLocalReads = allowShortCircuitLocalReads;
    return this;
  }

  public BlockReaderBatchFactory setClientCacheContext(
      ClientContext clientContext) {
    this.clientContext = clientContext;
    return this;
  }

  public BlockReaderBatchFactory setLength(long[] length) {
    this.length = length;
    return this;
  }

  public BlockReaderBatchFactory setCachingStrategy(
      CachingStrategy cachingStrategy) {
    this.cachingStrategy = cachingStrategy;
    return this;
  }

  public BlockReaderBatchFactory setInetSocketAddress (
      InetSocketAddress inetSocketAddress) {
    this.inetSocketAddress = inetSocketAddress;
    return this;
  }

  public BlockReaderBatchFactory setUserGroupInformation(
      UserGroupInformation userGroupInformation) {
    this.userGroupInformation = userGroupInformation;
    return this;
  }

  public BlockReaderBatchFactory setRemotePeerFactory(
      RemotePeerFactory remotePeerFactory) {
    this.remotePeerFactory = remotePeerFactory;
    return this;
  }

  public BlockReaderBatchFactory setConfiguration(
      Configuration configuration) {
    this.configuration = configuration;
    return this;
  }

  public BlockReaderBatchFactory setTracer(Tracer tracer) {
    this.tracer = tracer;
    return this;
  }

  @VisibleForTesting
  public static void setFailureInjectorForTesting(FailureInjector injector) {
    failureInjector = injector;
  }


  public BlockReader build() throws IOException {
    Preconditions.checkNotNull(configuration);

    return getRemoteBlockReaderFromTcp();
  }



  private BlockReader getRemoteBlockReaderFromTcp() throws IOException {
    LOG.trace("{}: trying to create a remote block reader from a TCP socket",
        this);
    BlockReader blockReader = null;
    while (true) {
      BlockReaderPeer curPeer = null;
      Peer peer = null;
      try {
        curPeer = nextTcpPeer();
        if (curPeer.fromCache) remainingCacheTries--;
        peer = curPeer.peer;
        blockReader = getRemoteBlockReader(peer);
        return blockReader;
      } catch (IOException ioe) {
        if (isSecurityException(ioe)) {
          LOG.trace("{}: got security exception while constructing a remote "
              + "block reader from {}", this, peer, ioe);
          throw ioe;
        }
        if ((curPeer != null) && curPeer.fromCache) {
          // Handle an I/O error we got when using a cached peer.  These are
          // considered less serious, because the underlying socket may be
          // stale.
          LOG.debug("Closed potentially stale remote peer {}", peer, ioe);
        } else {
          // Handle an I/O error we got when using a newly created peer.
          LOG.warn("I/O error constructing remote block reader.", ioe);
          throw ioe;
        }
      } finally {
        if (blockReader == null) {
          IOUtilsClient.cleanup(LOG, peer);
        }
      }
    }
  }

  public static class BlockReaderPeer {
    final Peer peer;
    final boolean fromCache;

    BlockReaderPeer(Peer peer, boolean fromCache) {
      this.peer = peer;
      this.fromCache = fromCache;
    }
  }

  /**
   * Get the next DomainPeer-- either from the cache or by creating it.
   *
   * @return the next DomainPeer, or null if we could not construct one.
   */
  private BlockReaderPeer nextDomainPeer() {
    if (remainingCacheTries > 0) {
      Peer peer = clientContext.getPeerCache().get(datanode, true);
      if (peer != null) {
        LOG.trace("nextDomainPeer: reusing existing peer {}", peer);
        return new BlockReaderPeer(peer, true);
      }
    }
    DomainSocket sock = clientContext.getDomainSocketFactory().
        createSocket(pathInfo, conf.getSocketTimeout());
    if (sock == null) return null;
    return new BlockReaderPeer(new DomainPeer(sock), false);
  }

  /**
   * Get the next TCP-based peer-- either from the cache or by creating it.
   *
   * @return the next Peer, or null if we could not construct one.
   *
   * @throws IOException  If there was an error while constructing the peer
   *                      (such as an InvalidEncryptionKeyException)
   */
  private BlockReaderPeer nextTcpPeer() throws IOException {
    if (remainingCacheTries > 0) {
      Peer peer = clientContext.getPeerCache().get(datanode, false);
      if (peer != null) {
        LOG.trace("nextTcpPeer: reusing existing peer {}", peer);
        return new BlockReaderPeer(peer, true);
      }
    }
    try {
      Peer peer = remotePeerFactory.newConnectedPeer(inetSocketAddress, token,
          datanode);
      LOG.trace("nextTcpPeer: created newConnectedPeer {}", peer);
      return new BlockReaderPeer(peer, false);
    } catch (IOException e) {
      LOG.trace("nextTcpPeer: failed to create newConnectedPeer connected to"
          + "{}", datanode);
      throw e;
    }
  }

  /**
   * Determine if an exception is security-related.
   *
   * We need to handle these exceptions differently than other IOExceptions.
   * They don't indicate a communication problem.  Instead, they mean that there
   * is some action the client needs to take, such as refetching block tokens,
   * renewing encryption keys, etc.
   *
   * @param ioe    The exception
   * @return       True only if the exception is security-related.
   */
  private static boolean isSecurityException(IOException ioe) {
    return (ioe instanceof InvalidToken) ||
            (ioe instanceof InvalidEncryptionKeyException) ||
            (ioe instanceof InvalidBlockTokenException) ||
            (ioe instanceof AccessControlException);
  }

  @SuppressWarnings("deprecation")
  private BlockReader getRemoteBlockReader(Peer peer) throws IOException {
    return BlockReaderRemote2Batch.newBlockReader(
            fileName, block, token, startOffset, length,
            verifyChecksum, clientName, peer, datanode,
            clientContext.getPeerCache(), cachingStrategy, tracer);
  }

  @Override
  public String toString() {
    return "BlockReaderFactory(fileName=" + fileName + ", block=" + block + ")";
  }

  /**
   * File name to print when accessing a block directly (from servlets)
   * @param s Address of the block location
   * @param poolId Block pool ID of the block
   * @param blockId Block ID of the block
   * @return string that has a file name for debug purposes
   */
  public static String getFileName(final InetSocketAddress s,
      final String poolId, final long blockId) {
    return s.toString() + ":" + poolId + ":" + blockId;
  }
}
