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
package org.apache.hadoop.fs;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/****************************************************************
 * FSInputStream is a generic old InputStream with a little bit
 * of RAF-style seek ability.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class FSInputStream extends InputStream
    implements Seekable, PositionedReadable {
  private static final Logger LOG =
      LoggerFactory.getLogger(FSInputStream.class);

  /**
   * Seek to the given offset from the start of the file.
   * The next read() will be from that location.  Can't
   * seek past the end of the file.
   */
  @Override
  public abstract void seek(long pos) throws IOException;

  /**
   * Return the current offset from the start of the file
   */
  @Override
  public abstract long getPos() throws IOException;

  /**
   * Seeks a different copy of the data.  Returns true if 
   * found a new source, false otherwise.
   */
  @Override
  public abstract boolean seekToNewSource(long targetPos) throws IOException;

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
    validatePositionedReadArgs(position, buffer, offset, length);
    if (length == 0) {
      return 0;
    }
    synchronized (this) {
      long oldPos = getPos();
      int nread = -1;
      try {
        seek(position);
        nread = read(buffer, offset, length);
      } catch (EOFException e) {
        // end of file; this can be raised by some filesystems
        // (often: object stores); it is swallowed here.
        LOG.debug("Downgrading EOFException raised trying to" +
            " read {} bytes at offset {}", length, offset, e);
      } finally {
        seek(oldPos);
      }
      return nread;
    }
  }

  /**
   * Validation code, available for use in subclasses.
   * @param position position: if negative an EOF exception is raised
   * @param buffer destination buffer
   * @param offset offset within the buffer
   * @param length length of bytes to read
   * @throws EOFException if the position is negative
   * @throws IndexOutOfBoundsException if there isn't space for the amount of
   * data requested.
   * @throws IllegalArgumentException other arguments are invalid.
   */
  protected void validatePositionedReadArgs(long position,
      byte[] buffer, int offset, int length) throws EOFException {
    Preconditions.checkArgument(length >= 0, "length is negative");
    if (position < 0) {
      throw new EOFException("position is negative");
    }
    Preconditions.checkArgument(buffer != null, "Null buffer");
    if (buffer.length - offset < length) {
      throw new IndexOutOfBoundsException(
          FSExceptionMessages.TOO_MANY_BYTES_FOR_DEST_BUFFER
              + ": request length=" + length
              + ", with offset ="+ offset
              + "; buffer capacity =" + (buffer.length - offset));
    }
  }

  protected void validatePositionedReadArgs(long[] position,
                                            byte[][] buffer, int[] offset, int[] length) throws EOFException {
   for(int i=0;i<position.length;i++)
   {
     Preconditions.checkArgument(length[i] >= 0, "length is negative");
     if (position[i] < 0) {
       throw new EOFException("position is negative");
     }
     Preconditions.checkArgument(buffer[i] != null, "Null buffer");
     if (buffer[i].length - offset[i] < length[i]) {
       throw new IndexOutOfBoundsException(
               FSExceptionMessages.TOO_MANY_BYTES_FOR_DEST_BUFFER
                       + ": request length=" + length[i]
                       + ", with offset ="+ offset[i]
                       + "; buffer capacity =" + (buffer[i].length - offset[i]));
     }
   }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException {
    validatePositionedReadArgs(position, buffer, offset, length);
    int nread = 0;
    while (nread < length) {
      int nbytes = read(position + nread,
          buffer,
          offset + nread,
          length - nread);
      if (nbytes < 0) {
        throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
      }
      nread += nbytes;
    }
  }


  public int cmpReadLength(int[] nread,int[] length)
  {
    int rtn=0;
  for(int i=0;i<nread.length;i++)
  {
      if(nread[i] < length[i])
      {
        rtn++;
      }
  }

    return rtn;
  }
  public void readFully(long[] position, byte[][] buffer, int[] offset, int[] length)
          throws IOException {
    validatePositionedReadArgs(position, buffer, offset, length);
    int[] nread = new int[position.length];
    Arrays.fill(nread,0);
    int nonZeroLen=cmpReadLength(nread , length);
    int loopcn=0;
    while (nonZeroLen>0) {

      if(loopcn++>10240)
      {
        throw  new IOException("too many loops");
      }
      long[] position_nonzero=new long[nonZeroLen];
      byte[][] buffer_nonzero=new byte[nonZeroLen][];
      int[] offset_nonzero=new int[nonZeroLen];
      int[] length_nonzero=new int[nonZeroLen];
      int[] index_list=new int[nonZeroLen];

      int index=0;
      for(int i=0;i<position.length;i++)
      {
        if(nread[i] >= length[i])
        {
          continue;
        }
        position_nonzero[index]=position[i]+nread[i];
        buffer_nonzero[index]=buffer[i];
        offset_nonzero[index]=offset[i]+nread[i];
        length_nonzero[index]=length[i]-+nread[i];
        index_list[index]=i;
        index++;
      }


      int[] nbytes = readBatch(position_nonzero, buffer_nonzero, offset_nonzero,length_nonzero);

      for(int i=0;i<nbytes.length;i++)
      {
        if (nbytes[i] < 0) {
          throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
        }
        nread[index_list[i]] += nbytes[i];
      }
      nonZeroLen=cmpReadLength(nread , length);
    }
  }

  public int[] readBatch(long[] position, byte[][] buffer, int[] offset, int[] length)
          throws IOException {
      throw new IOException("not supported");
  }

  @Override
  public void readFully(long position, byte[] buffer)
    throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }
}
