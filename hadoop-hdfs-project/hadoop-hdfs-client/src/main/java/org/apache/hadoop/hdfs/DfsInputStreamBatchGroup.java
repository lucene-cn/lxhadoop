package org.apache.hadoop.hdfs;

import java.util.Arrays;

public class DfsInputStreamBatchGroup {
    int index=0;

    @Override
    public String toString() {
        return "{" +
                "index=" + index +
                ", targetStart=" + targetStart +
                ", bytesToRead=" + bytesToRead +
                ", offset=" + offset +
                '}';
    }

    byte[] buffer;
    long targetStart;
    long bytesToRead;
    int offset;
}
