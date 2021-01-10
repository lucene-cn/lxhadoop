package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.HasFileDescriptor;

import java.io.*;
import java.nio.ByteBuffer;

public class LXLocalFSFileInputStream extends FSInputStream implements HasFileDescriptor {
    private FileInputStream fis;
    private long position;

    public LXLocalFSFileInputStream(File file) throws IOException {
        fis = new FileInputStream(file);
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos < 0) {
            throw new EOFException(
                    FSExceptionMessages.NEGATIVE_SEEK);
        }
        fis.getChannel().position(pos);
        this.position = pos;
    }

    @Override
    public long getPos() throws IOException {
        return this.position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    /*
     * Just forward to the fis
     */
    @Override
    public int available() throws IOException { return fis.available(); }
    @Override
    public void close() throws IOException { fis.close(); }
    @Override
    public boolean markSupported() { return false; }

    @Override
    public int read() throws IOException {
        try {
            int value = fis.read();
            if (value >= 0) {
                this.position++;
            }
            return value;
        } catch (IOException e) {                 // unexpected exception
            throw e;                   // assume native fs error
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        // parameter check
        validatePositionedReadArgs(position, b, off, len);
        try {
            int value = fis.read(b, off, len);
            if (value > 0) {
                this.position += value;
            }
            return value;
        } catch (IOException e) {                 // unexpected exception
            throw e;                   // assume native fs error
        }
    }

    @Override
    public int read(long position, byte[] b, int off, int len)
            throws IOException {
        // parameter check
        validatePositionedReadArgs(position, b, off, len);
        if (len == 0) {
            return 0;
        }

        ByteBuffer bb = ByteBuffer.wrap(b, off, len);
        try {
            int value = fis.getChannel().read(bb, position);
            if (value > 0) {
            }
            return value;
        } catch (IOException e) {
            throw e;
        }
    }

    @Override
    public long skip(long n) throws IOException {
        long value = fis.skip(n);
        if (value > 0) {
            this.position += value;
        }
        return value;
    }

    @Override
    public FileDescriptor getFileDescriptor() throws IOException {
        return fis.getFD();
    }
}
