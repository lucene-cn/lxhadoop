package org.apache.hadoop.hdfs;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.util.ArrayList;

public class BatchReadItems {

    public  ArrayList<DfsInputStreamBatchGroup> patch=new ArrayList<>();
    public LocatedBlock locate;

    public BatchReadItems(LocatedBlock locate) {
        this.locate = locate;
    }

}
