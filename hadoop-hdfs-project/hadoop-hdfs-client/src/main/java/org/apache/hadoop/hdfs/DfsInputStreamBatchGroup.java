package org.apache.hadoop.hdfs;

public class DfsInputStreamBatchGroup {
    int index=0;
    byte[] buffer;
    long targetStart;
    long bytesToRead;
    int offset;
}
