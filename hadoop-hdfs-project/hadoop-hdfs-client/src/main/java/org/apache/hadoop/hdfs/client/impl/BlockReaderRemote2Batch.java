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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.PeerCache;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.*;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientReadStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReadOpChecksumInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.UUID;

public class BlockReaderRemote2Batch implements BlockReader {

  static final Logger LOG = LoggerFactory.getLogger(BlockReaderRemote2Batch.class);
  static final int TCP_WINDOW_SIZE = 128 * 1024; // 128 KB;

  final private Peer peer;
  final private DatanodeID datanodeID;
  final private PeerCache peerCache;
  final private long blockId;
  private final DataInputStream inSocket;

  private final PacketReceiverBatch[] packetReceiver ;

  final ByteBuffer[] curDataSlice;



  /** offset in block where reader wants to actually read */
  private long[] startOffset;
  private final String filename;




  private final boolean isLocal;


  private boolean sentStatusCode = false;

  private final Tracer[] tracer;

  @VisibleForTesting
  public Peer getPeer() {
    return peer;
  }

  @Override
  public int readBatch(byte[][] buf, int[] off, int[] len) throws IOException {

    long ts=System.currentTimeMillis();
    byte[] byteBuffer=new byte[this.inSocket.readInt()];
    this.inSocket.readFully(byteBuffer,0,byteBuffer.length);

    DataInputStream inputRam=new DataInputStream(new ByteArrayInputStream(byteBuffer));
    int amt=0;
    for(int i=0;i<off.length;i++)
    {

      int nRead =inputRam.readInt();
      if(nRead!=len[i])
      {
        throw new IOException("check error :"+i+"@"+nRead+"@"+len);
      }
      inputRam.readFully(buf[i],off[i],nRead);

      amt+=nRead;
    }


    return amt;
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len)
      throws IOException {
    throw new IOException("not supported");
  }


  @Override
  public synchronized int read(ByteBuffer buf) throws IOException {
    throw new IOException("not supported");
  }


  @Override
  public synchronized long skip(long n) throws IOException {
    throw new IOException("not supported");

  }

  private void readTrailingEmptyPacket(int i,InputStream raminput) throws IOException {

    packetReceiver[i].receiveNextPacket(raminput);

    PacketHeaderBatch trailer = packetReceiver[i].getHeader();
    if (!trailer.isLastPacketInBlock() ||
        trailer.getDataLen() != 0) {
      throw new IOException("Expected empty end-of-read packet! Header: " +
          trailer);
    }
  }

  protected BlockReaderRemote2Batch(String file, long blockId,
                                    boolean verifyChecksum,
                                    long startOffset[],  long[] bytesToRead, Peer peer,
                                    DatanodeID datanodeID, PeerCache peerCache, Tracer tracer) throws IOException {
    this.isLocal = DFSUtilClient.isLocalAddress(NetUtils.
        createSocketAddr(datanodeID.getXferAddr()));
    // Path is used only for printing block and file information in debug
    this.peer = peer;
    this.datanodeID = datanodeID;

    this.inSocket = new DataInputStream(peer.getInputStream());// peer.getInputStreamChannel();
    this.startOffset =new long[startOffset.length];
    this.curDataSlice=new ByteBuffer[startOffset.length];

    for(int i=0;i<this.startOffset.length;i++)
    {
      this.startOffset[i] =Math.max(startOffset[i],0);

    }

    this.filename = file;
    this.peerCache = peerCache;
    this.blockId = blockId;

    // The total number of bytes that we need to transfer from the DN is
    // the amount that the user wants (bytesToRead), plus the padding at
    // the beginning in order to chunk-align. Note that the DN may elect
    // to send more than this amount if the read starts/ends mid-chunk.
    this.tracer=new Tracer[this.startOffset.length];
    for(int i=0;i<this.startOffset.length;i++)
    {
      this.tracer[i] = tracer;
    }

    this.packetReceiver=new PacketReceiverBatch[this.startOffset.length];
    for(int i=0;i<this.startOffset.length;i++)
    {
      this.packetReceiver[i]=new PacketReceiverBatch(false);
    }

  }


  @Override
  public synchronized void close() throws IOException {
    for(int i=0;i<this.startOffset.length;i++)
    {
      packetReceiver[i].close();

    }
    for(int i=0;i<startOffset.length;i++)
    {
      startOffset[i] = -1;

    }
    if (peerCache != null && sentStatusCode) {
      peerCache.put(datanodeID, peer);
    } else {
      peer.close();
    }


    // in will be closed when its Socket is closed.
  }

  /**
   * When the reader reaches end of the read, it sends a status response
   * (e.g. CHECKSUM_OK) to the DN. Failure to do so could lead to the DN
   * closing our connection (which we will re-open), but won't affect
   * data correctness.
   */
  void sendReadResult(Status statusCode) {
    assert !sentStatusCode : "already sent status code to " + peer;
    try {
      writeReadResult(peer.getOutputStream(), statusCode);
      sentStatusCode = true;
    } catch (IOException e) {
      // It's ok not to be able to send this. But something is probably wrong.
      LOG.info("Could not send read status (" + statusCode + ") to datanode " +
          peer.getRemoteAddressString() + ": " + e.getMessage());
    }
  }

  /**
   * Serialize the actual read result on the wire.
   */
  static void writeReadResult(OutputStream out, Status statusCode)
      throws IOException {

    ClientReadStatusProto.newBuilder()
        .setStatus(statusCode)
        .build()
        .writeDelimitedTo(out);

    out.flush();
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

  @Override
  public int readAll(byte[] buf, int offset, int len) throws IOException {
    throw new IOException("not supported");
  }

  @Override
  public void readFully(byte[] buf, int off, int len) throws IOException {
    throw new IOException("not supported");
  }


  public static BlockReader newBlockReader(String file,
      ExtendedBlock block,
      Token<BlockTokenIdentifier> blockToken,
      long[] startOffset, long[] len,
      boolean verifyChecksum,
      String clientName,
      Peer peer, DatanodeID datanodeID,
      PeerCache peerCache,
      CachingStrategy cachingStrategy,
      Tracer tracer) throws IOException {
    // in and out will be closed when sock is closed (by the caller)
    final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
        peer.getOutputStream()));
    new Sender(out).readBlockBatch(block, blockToken, clientName, startOffset, len,
        verifyChecksum, cachingStrategy);



    return new BlockReaderRemote2Batch(file, block.getBlockId(),
        verifyChecksum, startOffset, len, peer, datanodeID,
        peerCache, tracer);
  }

  static void checkSuccess(
          DataTransferProtos.BlockOpResponseBatchProto status, Peer peer,
          ExtendedBlock block, String file)
      throws IOException {
    String logInfo = "for OP_READ_BLOCK"
        + ", self=" + peer.getLocalAddressString()
        + ", remote=" + peer.getRemoteAddressString()
        + ", for file " + file
        + ", for pool " + block.getBlockPoolId()
        + " block " + block.getBlockId() + "_" + block.getGenerationStamp();
    DataTransferProtoUtil.checkBlockOpStatus(status, logInfo);
  }

  @Override
  public int available() {
    return TCP_WINDOW_SIZE;
  }

  @Override
  public boolean isLocal() {
    return isLocal;
  }

  @Override
  public boolean isShortCircuit() {
    return false;
  }

  @Override
  public ClientMmap getClientMmap(EnumSet<ReadOption> opts) {
    return null;
  }
}
