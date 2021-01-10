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
package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeaderBatch;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.DataChecksum;
import org.apache.htrace.core.TraceScope;
import org.slf4j.Logger;

import java.io.*;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

class BlockSenderBatch implements java.io.Closeable {
  static final Logger LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;
  private static final boolean is32Bit =System.getProperty("sun.arch.data.model").equals("32");
  /**
   * Minimum buffer used while sending data to clients. Used only if
   * transferTo() is enabled. 64KB is not that large. It could be larger, but
   * not sure if there will be much more improvement.
   */
  private static final int MIN_BUFFER_WITH_TRANSFERTO = 64*1024;
  private static final int IO_FILE_BUFFER_SIZE;
  static {
    HdfsConfiguration conf = new HdfsConfiguration();
    IO_FILE_BUFFER_SIZE = DFSUtilClient.getIoFileBufferSize(conf);
  }
  private static final int TRANSFERTO_BUFFER_SIZE = Math.max(
      IO_FILE_BUFFER_SIZE, MIN_BUFFER_WITH_TRANSFERTO);

  /** the block to read from */
  private final ExtendedBlock block;
  /** Stream to read block data from */
  private LXFSDataInputStream blockIn;
  /** updated while using transferTo() */
  private long blockInPosition = -1;
  /** Stream to read checksum */
  private LXFSDataInputStream checksumIn;
  /** Checksum utility */
  private final DataChecksum[] checksum;
  /** Initial position to read */
  private long[] initialOffset;
  /** Current position of read */
  private long offset[];
  /** Position of last byte to read from block file */
  private final long[] endOffset;
  /** Number of bytes in chunk used for computing checksum */
  private final int[] chunkSize;
  /** Number bytes of checksum computed for a chunk */
  private final int[] checksumSize;
  /** If true, failure to read checksum is ignored */
  private final boolean corruptChecksumOk;
  /** Sequence number of packet being sent */
  final  private long[] seqno;
  /** Set to true if transferTo is allowed for sending data to the client */
  private final boolean transferToAllowed;
  /** Set to true once entire requested byte range has been sent to the client */
  private boolean sentEntireByteRange;
  /** When true, verify checksum while reading from checksum file */
  private final boolean verifyChecksum;

  private volatile ChunkChecksum[] lastChunkChecksum;
  private DataNode datanode;


  /** The reference to the volume where the block is located */
  private FsVolumeReference volumeRef;

  /** The replica of the block that is being read. */
  private final Replica replica;


  private long[] lastCacheDropOffset;



  private static final long CHUNK_SIZE = 512;

  long[] checksumSkip;
  BlockSenderBatch(ExtendedBlock block, long[] startOffset, long[] length,
                   boolean corruptChecksumOk, boolean verifyChecksum,
                   boolean sendChecksum, DataNode datanode, String clientTraceFmt,
                   CachingStrategy cachingStrategy)
      throws IOException {
    try {
      this.block = block;
      this.corruptChecksumOk = corruptChecksumOk;
      this.verifyChecksum = verifyChecksum;

      this.datanode = datanode;
      
      if (verifyChecksum) {
        Preconditions.checkArgument(sendChecksum, "If verifying checksum, currently must also send it.");
      }

      ChunkChecksum chunkChecksum = null;
      final long replicaVisibleLength;
      try(AutoCloseableLock lock = datanode.data.acquireDatasetLock()) {
        replica = getReplica(block, datanode);
        replicaVisibleLength = replica.getVisibleLength();
      }
      long maxLen=0l;
      long maxBlockLen=0l;
      for(int i=0;i<startOffset.length;i++)
      {
        maxBlockLen=Math.max(maxBlockLen,startOffset[i] + length[i]);
        maxLen=Math.max(maxLen,length[i]);
      }
      // if there is a write in progress
      if (replica instanceof ReplicaBeingWritten) {
        final ReplicaBeingWritten rbw = (ReplicaBeingWritten)replica;
        waitForMinLength(rbw, maxBlockLen);
        chunkChecksum = rbw.getLastChecksumAndDataLen();
      }
      if (replica instanceof FinalizedReplica) {
        chunkChecksum = getPartialChunkChecksumForFinalized((FinalizedReplica)replica);
      }

      if (replica.getGenerationStamp() < block.getGenerationStamp()) {
        throw new IOException("Replica gen stamp < block genstamp, block="
            + block + ", replica=" + replica);
      } else if (replica.getGenerationStamp() > block.getGenerationStamp()) {
        if (DataNode.LOG.isDebugEnabled()) {
          DataNode.LOG.debug("Bumping up the client provided"
              + " block's genstamp to latest " + replica.getGenerationStamp()
              + " for block " + block);
        }
        block.setGenerationStamp(replica.getGenerationStamp());
      }
      if (replicaVisibleLength < 0) {
        throw new IOException("Replica is not readable, block="
            + block + ", replica=" + replica);
      }
      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("block=" + block + ", replica=" + replica);
      }

      // transferToFully() fails on 32 bit platforms for block sizes >= 2GB,
      // use normal transfer in those cases
      this.transferToAllowed = datanode.getDnConf().transferToAllowed &&
        (!is32Bit || maxLen <= Integer.MAX_VALUE);

      // Obtain a reference before reading data
      this.volumeRef = datanode.data.getVolume(block).obtainReference();

      /* 
       * (corruptChecksumOK, meta_file_exist): operation
       * True,   True: will verify checksum  
       * True,  False: No verify, e.g., need to read data from a corrupted file 
       * False,  True: will verify checksum
       * False, False: throws IOException file not found
       */
      DataChecksum[] csum = new DataChecksum[startOffset.length];
      this.checksum=new DataChecksum[startOffset.length];
      this.chunkSize=new int[startOffset.length];
      this.lastChunkChecksum=new ChunkChecksum[startOffset.length];
      if (verifyChecksum || sendChecksum) {
        File metaIn = null;
        boolean keepMetaInOpen = false;
        try {
          DataNodeFaultInjector.get().throwTooManyOpenFiles();
          metaIn = datanode.data.getMetaRandomAccessFile(block);
          if (!corruptChecksumOk || metaIn != null) {
            if (metaIn == null) {
              //need checksum but meta-data not found
              throw new FileNotFoundException("Meta-data not found for " +
                  block);
            }


            if (!replica.isOnTransientStorage() &&
                metaIn.length() >= BlockMetadataHeader.getHeaderSize()) {
              this.checksumIn=new LXFSDataInputStream(new LXBufferedFSInputStream(new LXLocalFSFileInputStream(metaIn), 1024));
                  // HDFS-11160/HDFS-11056
              DataNodeFaultInjector.get() .waitForBlockSenderMetaInputStreamBeforeAppend();

              DataChecksum  csum_share = BlockMetadataHeader.readDataChecksum(this.checksumIn, block);
              for(int i=0;i<startOffset.length;i++)
              {
                csum[i]=csum_share;
              }

              keepMetaInOpen = true;
            }
          } else {
            LOG.warn("Could not find metadata file for " + block);
          }
        } catch (FileNotFoundException e) {
          if ((e.getMessage() != null) && !(e.getMessage()
              .contains("Too many open files"))) {
            // The replica is on its volume map but not on disk
            datanode.notifyNamenodeDeletedBlock(block, replica.getStorageUuid());
            datanode.data.invalidate(block.getBlockPoolId(),
                new Block[] {block.getLocalBlock()});
          }
          throw e;
        } finally {
          if (!keepMetaInOpen) {
            this.checksumIn.close();
            checksumIn = null;

          }
        }
      }
      if (csum == null) {
        for(int i=0;i<startOffset.length;i++)
        {
          csum[i]=DataChecksum.newDataChecksum(DataChecksum.Type.NULL,(int)CHUNK_SIZE);
        }
      }

      /*
       * If chunkSize is very large, then the metadata file is mostly
       * corrupted. For now just truncate bytesPerchecksum to blockLength.
       */       
      int size = csum[0].getBytesPerChecksum();
      if (size > 10*1024*1024 && size > replicaVisibleLength) {
        for(int i=0;i<startOffset.length;i++)
        {
          csum[i] = DataChecksum.newDataChecksum(csum[0].getChecksumType(), Math.max((int)replicaVisibleLength, 10*1024*1024));
        }
        size = csum[0].getBytesPerChecksum();
      }
      Arrays.fill(chunkSize,size);
      checksumSize=new int[startOffset.length];
      for(int i=0;i<startOffset.length;i++)
      {
        checksum[i] = csum[i];
        checksumSize[i] = checksum[i].getChecksumSize();

      }

      // end is either last byte on disk or the length for which we have a
      // checksum
      long init_end=chunkChecksum != null ? chunkChecksum.getDataLength()     : replica.getBytesOnDisk();
      long[] end = new long[startOffset.length];
      Arrays.fill(end,init_end);
      offset=new long[startOffset.length];
      endOffset=new long[startOffset.length];
      checksumSkip=new long[startOffset.length];
      seqno=new long[startOffset.length];

//2021-01-10 18:18:57,299 INFO org.apache.hadoop.hdfs.server.datanode.DataNode: yanniandebug:134217728@134217728@132766993@1022
      LOG.info("yanniandebug_1:"+replica.getBytesOnDisk()+"@"+end+"@"+maxBlockLen+"@"+maxLen+"@"+checksumSize);
      for(int i=0;i<startOffset.length;i++)
      {
        length[i] = length[i] < 0 ? replicaVisibleLength : length[i];
//java.io.IOException:  Offset 107810760 and length 875 don't match block BP-1698889102-192.168.0.191-1594708809587:blk_1073764539_23715 ( blockLen 6375424 )
        if (startOffset[i] < 0 || startOffset[i] > end[i]       || (length[i] + startOffset[i]) > end[i]) {
          String msg = " Offset " + startOffset[i] + " and length " + length[i]
                  + " don't match block " + block + " ( blockLen " + end[i] + " )";
          LOG.warn(datanode.getDNRegistrationForBP(block.getBlockPoolId()) +
                  ":sendBlock() : " + msg);
          throw new IOException(msg);
        }

        // Ensure read offset is position at the beginning of chunk
        offset[i] = startOffset[i] - (startOffset[i] % chunkSize[i]);
        if (length[i] >= 0) {
          // Ensure endOffset points to end of chunk.
          long tmpLen = startOffset[i] + length[i];
          if (tmpLen % chunkSize[i] != 0) {
            tmpLen += (chunkSize[i] - tmpLen % chunkSize[i]);
          }
          if (tmpLen < end[i]) {
            // will use on-disk checksum here since the end is a stable chunk
            end[i] = tmpLen;
          } else if (chunkChecksum != null) {
            // last chunk is changing. flag that we need to use in-memory checksum
            this.lastChunkChecksum[i] = chunkChecksum;
          }
        }
        endOffset[i] = end[i];

        // seek to the right offsets
        if (offset[i] > 0 && checksumIn != null) {
          checksumSkip[i]=(offset[i] / chunkSize[i]) * checksumSize[i];


        }
      }


      Arrays.fill(seqno,0);

      this.blockIn=new LXFSDataInputStream(new LXBufferedFSInputStream(new LXLocalFSFileInputStream(datanode.data.getBlockRandomAccessFile(block)), 1024));


    } catch (IOException ioe) {
      IOUtils.closeStream(this);
      throw ioe;
    }



  }

  private ChunkChecksum getPartialChunkChecksumForFinalized(
      FinalizedReplica finalized) throws IOException {
    // There are a number of places in the code base where a finalized replica
    // object is created. If last partial checksum is loaded whenever a
    // finalized replica is created, it would increase latency in DataNode
    // initialization. Therefore, the last partial chunk checksum is loaded
    // lazily.

    // Load last checksum in case the replica is being written concurrently
    final long replicaVisibleLength = replica.getVisibleLength();
    if (replicaVisibleLength % CHUNK_SIZE != 0 &&
        finalized.getLastPartialChunkChecksum() == null) {
      // the finalized replica does not have precomputed last partial
      // chunk checksum. Recompute now.
      try {
        finalized.loadLastPartialChunkChecksum();
        return new ChunkChecksum(finalized.getVisibleLength(),
            finalized.getLastPartialChunkChecksum());
      } catch (FileNotFoundException e) {
        // meta file is lost. Continue anyway to preserve existing behavior.
        DataNode.LOG.warn(
            "meta file " + finalized.getMetaFile() + " is missing!");
        return null;
      }
    } else {
      // If the checksum is null, BlockSender will use on-disk checksum.
      return new ChunkChecksum(finalized.getVisibleLength(),
          finalized.getLastPartialChunkChecksum());
    }
  }

  /**
   * close opened files.
   */
  @Override
  public void close() throws IOException {

    IOException ioe = null;
    if(checksumIn!=null) {
      try {
        checksumIn.close(); // close checksum file
      } catch (IOException e) {
        ioe = e;
      }
      checksumIn = null;
    }   
    if(blockIn!=null) {
      try {
        blockIn.close(); // close data file
      } catch (IOException e) {
        ioe = e;
      }
      blockIn = null;
    }
    if (volumeRef != null) {
      IOUtils.cleanup(null, volumeRef);
      volumeRef = null;
    }
    // throw IOException if there is any
    if(ioe!= null) {
      throw ioe;
    }
  }
  
  private static Replica getReplica(ExtendedBlock block, DataNode datanode)
      throws ReplicaNotFoundException {
    Replica replica = datanode.data.getReplica(block.getBlockPoolId(),
        block.getBlockId());
    if (replica == null) {
      throw new ReplicaNotFoundException(block);
    }
    return replica;
  }
  
  /**
   * Wait for rbw replica to reach the length
   * @param rbw replica that is being written to
   * @param len minimum length to reach
   * @throws IOException on failing to reach the len in given wait time
   */
  private static void waitForMinLength(ReplicaBeingWritten rbw, long len)
      throws IOException {
    // Wait for 3 seconds for rbw replica to reach the minimum length
    for (int i = 0; i < 30 && rbw.getBytesOnDisk() < len; i++) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    long bytesOnDisk = rbw.getBytesOnDisk();
    if (bytesOnDisk < len) {
      throw new IOException(
          String.format("Need %d bytes, but only %d bytes available", len,
              bytesOnDisk));
    }
  }

  /**
   * Converts an IOExcpetion (not subclasses) to SocketException.
   * This is typically done to indicate to upper layers that the error 
   * was a socket error rather than often more serious exceptions like 
   * disk errors.
   */
  private static IOException ioeToSocketException(IOException ioe) {
    if (ioe.getClass().equals(IOException.class)) {
      // "se" could be a new class in stead of SocketException.
      IOException se = new SocketException("Original Exception : " + ioe);
      se.initCause(ioe);
      /* Change the stacktrace so that original trace is not truncated
       * when printed.*/ 
      se.setStackTrace(ioe.getStackTrace());
      return se;
    }
    // otherwise just return the same exception.
    return ioe;
  }

  /**
   * @param datalen Length of data 
   * @return number of chunks for data of given size
   */
  private int numberOfChunks(int index,long datalen) {
    return (int) ((datalen + chunkSize[index] - 1)/chunkSize[index]);
  }
  

  private int sendPacket(int index,ByteBuffer pkt, int maxChunks, OutputStream out,
      boolean transferTo, DataTransferThrottler throttler) throws IOException {
    int dataLen = (int) Math.min(endOffset[index] - offset[index], (chunkSize[index] * (long) maxChunks));
    
    int numChunks = numberOfChunks(index,dataLen); // Number of chunks be sent in the packet
    int checksumDataLen = numChunks * checksumSize[index];
    int packetLen = dataLen + checksumDataLen + 4;
    //单条消息的最后一个包
    boolean lastDataPacket = offset[index]+ dataLen == endOffset[index] && dataLen > 0;

    LOG.info("yanniandebug 1 writePacketHeader "+index);
    int headerLen = writePacketHeader(pkt, dataLen,index, packetLen);
    
    // Per above, the header doesn't start at the beginning of the
    // buffer
    int headerOff = pkt.position() - headerLen;
    
    int checksumOff = pkt.position();
    byte[] buf = pkt.array();
    
    if (checksumSize[index] > 0 && checksumIn != null) {
      LOG.info("yanniandebug 2 readChecksum "+index);
      readChecksum(index,buf, checksumOff, checksumDataLen);

      // write in progress that we need to use to get last checksum
      if (lastDataPacket && lastChunkChecksum != null) {
        LOG.info("yanniandebug 3 lastDataPacket "+index);

        int start = checksumOff + checksumDataLen - checksumSize[index];
        byte[] updatedChecksum = lastChunkChecksum[index].getChecksum();
        if (updatedChecksum != null) {
          System.arraycopy(updatedChecksum, 0, buf, start, checksumSize[index]);
        }
      }
    }
    
    int dataOff = checksumOff + checksumDataLen;
    if (!transferTo) { // normal transfer
//      IOUtils.readFully(in, buf, dataOff, dataLen);
      LOG.info("yanniandebug 4 readFully "+index +"@"+dataOff+"@"+dataLen);

      this.blockIn.readFully(buf, dataOff, dataLen);
      if (verifyChecksum) {
        LOG.info("yanniandebug 5 verifyChecksum "+index +"@"+dataOff+"@"+dataLen);

        verifyChecksum(index,buf, dataOff, dataLen, numChunks, checksumOff);
      }
    }
    
    try {
      if (transferTo) {
        throw new IOException("not supported");
//        SocketOutputStream sockOut = (SocketOutputStream)out;
//        // First write header and checksums
//        sockOut.write(buf, headerOff, dataOff - headerOff);
//
//        // no need to flush since we know out is not a buffered stream
//        FileChannel fileCh = (blockIn).getChannel();
//        LongWritable waitTime = new LongWritable();
//        LongWritable transferTime = new LongWritable();
//        sockOut.transferToFully(fileCh, blockInPosition, dataLen,
//            waitTime, transferTime);
//        datanode.metrics.addSendDataPacketBlockedOnNetworkNanos(waitTime.get());
//        datanode.metrics.addSendDataPacketTransferNanos(transferTime.get());
//        blockInPosition += dataLen;
      } else {
        LOG.info("yanniandebug 6 write "+index +"@"+dataOff+"@"+dataLen);

        // normal transfer
        out.write(buf, headerOff, dataOff + dataLen - headerOff);
      }
    } catch (IOException e) {
      if (e instanceof SocketTimeoutException) {
      } else {
        String ioem = e.getMessage();
        if (!ioem.startsWith("Broken pipe") && !ioem.startsWith("Connection reset")) {
          LOG.error("BlockSender.sendChunks() exception: ", e);
          datanode.getBlockScanner().markSuspectBlock(
              volumeRef.getVolume().getStorageID(),
              block);
        }
      }
      throw ioeToSocketException(e);
    }

    if (throttler != null) { // rebalancing so throttle
      throttler.throttle(packetLen);
    }

    return dataLen;
  }
  
  /**
   * Read checksum into given buffer
   * @param buf buffer to read the checksum into
   * @param checksumOffset offset at which to write the checksum into buf
   * @param checksumLen length of checksum to write
   * @throws IOException on error
   */
  private void readChecksum(int index,byte[] buf, final int checksumOffset,
      final int checksumLen) throws IOException {
    if (checksumSize[index] <= 0 && checksumIn == null) {
      return;
    }
    try {
      checksumIn.readFully(buf, checksumOffset, checksumLen);
    } catch (IOException e) {
      LOG.warn(" Could not read or failed to verify checksum for data"
          + " at offset " + offset + " for block " + block, e);
      IOUtils.closeStream(checksumIn);
      checksumIn = null;
      if (corruptChecksumOk) {
        if (checksumOffset < checksumLen) {
          // Just fill the array with zeros.
          Arrays.fill(buf, checksumOffset, checksumLen, (byte) 0);
        }
      } else {
        throw e;
      }
    }
  }
  
  /**
   * Compute checksum for chunks and verify the checksum that is read from
   * the metadata file is correct.
   * 
   * @param buf buffer that has checksum and data
   * @param dataOffset position where data is written in the buf
   * @param datalen length of data
   * @param numChunks number of chunks corresponding to data
   * @param checksumOffset offset where checksum is written in the buf
   * @throws ChecksumException on failed checksum verification
   */
  public void verifyChecksum(int index,final byte[] buf, final int dataOffset,
      final int datalen, final int numChunks, final int checksumOffset)
      throws ChecksumException {
    int dOff = dataOffset;
    int cOff = checksumOffset;
    int dLeft = datalen;

    for (int i = 0; i < numChunks; i++) {
      checksum[index].reset();
      int dLen = Math.min(dLeft, chunkSize[index]);
      checksum[index].update(buf, dOff, dLen);
      if (!checksum[index].compare(buf, cOff)) {
        long failedPos = offset[i] + datalen - dLeft;
        StringBuilder replicaInfoString = new StringBuilder();
        if (replica != null) {
          replicaInfoString.append(" for replica: " + replica.toString());
        }
        throw new ChecksumException("Checksum failed at " + failedPos
            + replicaInfoString, failedPos);
      }
      dLeft -= dLen;
      dOff += dLen;
      cOff += checksumSize[index];
    }
  }
  
  /**
   * sendBlock() is used to read block and its metadata and stream the data to
   * either a client or to another datanode. 
   * 
   * @param out  stream to which the block is written to
   * @param baseStream optional. if non-null, <code>out</code> is assumed to 
   *        be a wrapper over this stream. This enables optimizations for
   *        sending the data, e.g. 
   *        {@link SocketOutputStream#transferToFully(FileChannel, 
   *        long, int)}.
   * @param throttler for sending data.
   * @return total bytes read, including checksum data.
   */
  long sendBlock(DataOutputStream out, OutputStream baseStream, 
                 DataTransferThrottler throttler) throws IOException {
    TraceScope scope = datanode.tracer.
        newScope("sendBlock_" + block.getBlockId());
    try {
      return doSendBlock(out, baseStream, throttler);
    } finally {
      scope.close();
    }
  }

  private long doSendBlock(DataOutputStream out, OutputStream baseStream,
        DataTransferThrottler throttler) throws IOException {
    if (out == null) {
      throw new IOException( "out stream is null" );
    }
    initialOffset=new long[offset.length];
    lastCacheDropOffset=new long[offset.length];
    long totalRead = 0;

    final long lastPos=checksumIn.getPos();
    try {
      for(int i=0;i<offset.length;i++)
      {
LOG.info("yanniandebug send:"+i);
        this.blockIn.seek(this.offset[i]);
        this.checksum[i].reset();
        checksumIn.seek(lastPos);
        // note blockInStream is seeked when created below
        if (checksumSkip[i] > 0) {
          // Should we use seek() for checksum file as well?
          IOUtils.skipFully(checksumIn, checksumSkip[i]);
        }



          initialOffset[i] = offset[i];
          lastCacheDropOffset[i] = initialOffset[i];


          OutputStream streamForSendChunks = out;


          int maxChunksPerPacket;
          int pktBufSize = PacketHeaderBatch.PKT_MAX_HEADER_LEN;
          boolean transferTo = false;//transferToAllowed && !verifyChecksum && baseStream instanceof SocketOutputStream;
          if (transferTo) {
            throw new IOException("not supported");
//
////            FileChannel fileChannel = blockIn.getChannel();
//            blockInPosition =blockIn.getPos();//fileChannel.position();
//            streamForSendChunks = baseStream;
//            maxChunksPerPacket = numberOfChunks(TRANSFERTO_BUFFER_SIZE);
//
//            // Smaller packet size to only hold checksum when doing transferTo
//            pktBufSize += checksumSize * maxChunksPerPacket;
          } else {
            maxChunksPerPacket = Math.max(1, numberOfChunks(i,IO_FILE_BUFFER_SIZE));
            // Packet size includes both checksum and data
            pktBufSize += (chunkSize[i] + checksumSize[i]) * maxChunksPerPacket;
          }

          ByteBuffer pktBuf = ByteBuffer.allocate(pktBufSize);

          while (endOffset[i] > offset[i] && !Thread.currentThread().isInterrupted()) {
            long len = sendPacket(i,pktBuf, maxChunksPerPacket, streamForSendChunks, transferTo, throttler);
            offset[i] += len;
            totalRead += len + (numberOfChunks(i,len) * checksumSize[i]);
            seqno[i]++;
          }
          // If this thread was interrupted, then it did not send the full block.
          if (!Thread.currentThread().isInterrupted()) {
            try {
              LOG.info("yanniandebug sendempty:"+i);

              // send an empty packet to mark the end of the block
              sendPacket(i,pktBuf, maxChunksPerPacket, streamForSendChunks, transferTo,  throttler);
            } catch (IOException e) { //socket error
              throw ioeToSocketException(e);
            }

          }


      }

      sentEntireByteRange = true;

    } finally {
      out.flush();
      close();
    }
    return totalRead;
  }

  private int writePacketHeader(ByteBuffer pkt, int dataLen,int index, int packetLen) {
    pkt.clear();
    // both syncBlock and syncPacket are false
    PacketHeaderBatch header = new PacketHeaderBatch(packetLen, offset[index], seqno[index],index,
        (dataLen == 0), dataLen, false);

    int size = header.getSerializedSize();
    //2021-01-10 21:03:12,238 INFO org.apache.hadoop.hdfs.server.datanode.DataNode: yanniandebug:33@40@545@1024@0@1036
    LOG.info("yanniandebug:"+PacketHeaderBatch.PKT_MAX_HEADER_LEN +"@"+ size+"@"+pkt.capacity()+"@"+dataLen+"@"+index+"@"+packetLen);

    pkt.position(PacketHeaderBatch.PKT_MAX_HEADER_LEN - size);
    header.putInBuffer(pkt);
    return size;
  }
  
  boolean didSendEntireByteRange() {
    return sentEntireByteRange;
  }

  DataChecksum[] getChecksum() {
    return checksum;
  }
  long[] getOffset() {
    return offset;
  }
}
