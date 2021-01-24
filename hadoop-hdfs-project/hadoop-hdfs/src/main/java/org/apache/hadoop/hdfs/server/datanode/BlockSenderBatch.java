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

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.htrace.core.TraceScope;
import org.slf4j.Logger;

import java.io.*;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class BlockSenderBatch implements java.io.Closeable {
  static final Logger LOG = DataNode.LOG;

  private static final int GROUP_BATCH = 128;


  /** the block to read from */
  private final ExtendedBlock block;
  /** Stream to read block data from */
  private LXFSDataInputStream[] blockIn_group;
  /** Current position of read */
  private long offset[];
  /** Position of last byte to read from block file */
  private final long[] endOffset;


  private DataNode datanode;


  /** The reference to the volume where the block is located */
  private FsVolumeReference volumeRef;

  /** The replica of the block that is being read. */
  private final Replica replica;


  int total_len=0;
  BlockSenderBatch(ExtendedBlock block, long[] startOffset, long[] length,
                   boolean corruptChecksumOk, boolean verifyChecksum,
                   boolean sendChecksum, DataNode datanode, String clientTraceFmt,
                   CachingStrategy cachingStrategy)
      throws IOException {
    try {
      this.block = block;
      this.datanode = datanode;

      final long replicaVisibleLength;
      try(AutoCloseableLock lock = datanode.data.acquireDatasetLock()) {
        replica = getReplica(block, datanode);
        replicaVisibleLength = replica.getVisibleLength();
      }
      long maxLen=0l;
      long maxBlockLen=0l;
      this.total_len=0;
      for(int i=0;i<startOffset.length;i++)
      {
        maxBlockLen=Math.max(maxBlockLen,startOffset[i] + length[i]);
        maxLen=Math.max(maxLen,length[i]);
        this.total_len+=length[i];
      }
      // if there is a write in progress
      if (replica instanceof ReplicaBeingWritten) {
        final ReplicaBeingWritten rbw = (ReplicaBeingWritten)replica;
        waitForMinLength(rbw, maxBlockLen);
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

      // Obtain a reference before reading data
      this.volumeRef = datanode.data.getVolume(block).obtainReference();

      long init_end= replica.getBytesOnDisk();
      long[] end = new long[startOffset.length];
      Arrays.fill(end,init_end);
      this.offset=new long[startOffset.length];
      this.endOffset=new long[startOffset.length];

      for(int i=0;i<startOffset.length;i++)
      {
        length[i] = length[i] < 0 ? replicaVisibleLength : length[i];
        if (startOffset[i] < 0 || startOffset[i] > end[i]       || (length[i] + startOffset[i]) > end[i]) {
          String msg = " Offset " + startOffset[i] + " and length " + length[i]
                  + " don't match block " + block + " ( blockLen " + end[i] + " )";
          LOG.warn(datanode.getDNRegistrationForBP(block.getBlockPoolId()) +
                  ":sendBlock() : " + msg);
          throw new IOException(msg);
        }

        //因为checksum对齐,在随机读的逻辑里没必要,太影响性能了,因此先不进行checksum的校验了,后期可以考虑,在server端校验,客户端校验传输流即可
        this.offset[i] = startOffset[i];// - (startOffset[i] % chunkSize[i]);
        if (length[i] >= 0) {
          long tmpLen = startOffset[i] + length[i];
          if (tmpLen < end[i]) {
            end[i] = tmpLen;
          }
        }

        this.endOffset[i] = end[i];
      }

      this.blockIn_group=new LXFSDataInputStream[1+(startOffset.length/GROUP_BATCH)];
      for(int g=0;g<this.blockIn_group.length;g++)
      {
          this.blockIn_group[g] =new LXFSDataInputStream(new LXBufferedFSInputStream(new LXLocalFSFileInputStream(datanode.data.getBlockRandomAccessFile(block)), 1024));
      }

    } catch (IOException ioe) {
      IOUtils.closeStream(this);
      throw ioe;
    }

  }


  /**
   * close opened files.
   */
  @Override
  public void close() throws IOException {

    IOException ioe = null;

    for(int g=0;g<blockIn_group.length;g++)
    {
      if(blockIn_group[g]!=null) {
        try {
          blockIn_group[g].close(); // close data file
        } catch (IOException e) {
          ioe = e;
        }
        blockIn_group[g] = null;
      }
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


  final static AtomicReference<ExecutorService> threadPoolSplit = new AtomicReference<>(null);

  final static Object LCK=new Object();
  public static ExecutorService GET_POOL()
  {

    ExecutorService rtn=threadPoolSplit.get();
    if(rtn==null)
    {
      synchronized (LCK)
      {
        rtn=threadPoolSplit.get();
        if(rtn==null)
        {
          int thrs=Math.max(Runtime.getRuntime().availableProcessors()*4,12);

          LOG.info("batch_process_thr:"+thrs);
          rtn=Executors.newFixedThreadPool(thrs);
          threadPoolSplit.set(rtn);
        }
      }
    }

    return rtn;

  }

  private Object doSendGroup(int gstart, int gend, DataOutputStream out, final DataTransferThrottler throttler,  final AtomicLong totalRead) throws IOException {
    int maxDataLen=0;
    for(int i=gstart;i<gend;i++)
    {
      int dataLen = (int) (endOffset[i] - offset[i]);// Math.min(, (chunkSize[index] * (long) maxChunks));
      maxDataLen=Math.max(maxDataLen,dataLen);

    }

    byte[] data=new byte[maxDataLen];

    for(int i=gstart;i<gend;i++)
    {
      int gid=i/GROUP_BATCH;
      blockIn_group[gid].seek(offset[i]);
      int dataLen = (int) (endOffset[i] - offset[i]);// Math.min(, (chunkSize[index] * (long) maxChunks));
        try {
          this.blockIn_group[gid].readFully(data, 0, dataLen);
          out.writeInt(dataLen);
          out.write(data, 0, dataLen);
        } catch (IOException e) {
          if (e instanceof SocketTimeoutException) {
          } else {
            String ioem = e.getMessage();
            if (!ioem.startsWith("Broken pipe") && !ioem.startsWith("Connection reset")) {
              LOG.error("BlockSender.sendChunks() exception: ", e);
              datanode.getBlockScanner().markSuspectBlock(volumeRef.getVolume().getStorageID(),block);
            }
          }
          throw ioeToSocketException(e);
        }

        if (throttler != null) { // rebalancing so throttle
          throttler.throttle(dataLen);
        }

        offset[i] += dataLen;
        totalRead.addAndGet(dataLen);


    }
    return new Object();
  }
  private long doSendBlock(DataOutputStream outbuffer, OutputStream baseStream,    final DataTransferThrottler throttler) throws IOException {
    if (outbuffer == null) {
      throw new IOException( "out stream is null" );
    }

    final AtomicLong totalRead = new AtomicLong(0);

    int currentGroup=-1;
    int groupStart=0;

    final List<Future<Object>> futures = new ArrayList<Future<Object>>(1+(offset.length/GROUP_BATCH));
    ExecutorService pool=GET_POOL();

    ArrayList<ByteArrayOutputStream> list=new ArrayList<>();

    for(int i=0;i<offset.length;i++)
    {
      if(currentGroup==-1)
      {
        groupStart=i;
        currentGroup=i/GROUP_BATCH;
        continue;
      }
      int group=i/GROUP_BATCH;
      if(currentGroup>=0&&currentGroup!=group)
      {
        final int gstart=groupStart;
        final int gend=i;
        groupStart=i;
        currentGroup=group;

        final ByteArrayOutputStream out=new ByteArrayOutputStream(1024);
        list.add(out);

        futures.add(pool.submit(new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            DataOutputStream output=new DataOutputStream(out);
            try {
               doSendGroup(gstart, gend, output, throttler, totalRead);
            }finally
            {
              output.close();
            }

            return new Object();
          }
        }));

      }

    }

    {
      final int gstart=groupStart;
      final int gend=offset.length;

      if(gstart<gend)
      {
        final ByteArrayOutputStream out=new ByteArrayOutputStream(1024);
        list.add(out);

        futures.add(pool.submit(new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            DataOutputStream output=new DataOutputStream(out);
            try {
              doSendGroup(gstart, gend, output, throttler, totalRead);
            }finally
            {
              output.close();
            }
            return new Object();
          }
        }));
      }

    }


    try {
      for (Future<Object> future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          throw (InterruptedIOException) new InterruptedIOException().initCause(e);
        } catch (ExecutionException e) {
          throw new IOException(e);
        }
      }


    } finally {

      int sumsz=0;
      for(ByteArrayOutputStream out:list)
      {
        int sz=out.size();
        sumsz+=sz;
      }

      outbuffer.writeInt(sumsz);


      for(ByteArrayOutputStream out:list)
      {
        out.writeTo(outbuffer);
      }

      outbuffer.flush();
      close();

    }
    return totalRead.get();
  }


  long[] getOffset() {
    return offset;
  }
}
