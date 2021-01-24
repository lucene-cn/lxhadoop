package cn.lucene.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;

public class BatchRead {
	public static void main(String[] args) throws IOException {
		FileSystem fs=FileSystem.get(new Configuration());
		
		FSDataInputStream input=fs.open(new Path(args[0]));
		
		long max_position=fs.getLength(new Path(args[0]))-1024*1024*128;
		
		InputStream raw = input.getWrappedStream();
		System.out.println(input.getClass().getName());

		System.out.println(raw.getClass().getName());

		if(raw instanceof DFSInputStream)
		{
			//这里支持批量读
			FSInputStream batch=(DFSInputStream)raw;
			System.out.println(batch.getClass().getName());
			
			int batchCnt=10000;//1024+(int) (Math.random()*10240);;
			long[] position=new long[batchCnt];
			byte[][] buffer=new byte[batchCnt][];
			int[] start=new int[batchCnt];
			int[] length=new int[batchCnt];
			byte[][] buffers=new byte[batchCnt][];

			for(int i=0;i<10000;i++)
			{
				

				long pos=(long) (Math.random()*max_position);
				for(int j=0;j<batchCnt;j++)
				{
					position[j]=1+pos+(long) (Math.random()*1024*1024*128);//随机数不能越界 //1+(long) (Math.random()*max_position);//
					
					int len=64;//(int) (Math.random()*1024);
					if(buffer[j]==null)
					{
						buffer[j]=new byte[len];
					}
					start[j]=0;
					length[j]=len;
				}
				
				long ts=System.currentTimeMillis();

				//真正的使用API
				batch.readFully(position, buffer, start, length);
				long ts1=System.currentTimeMillis();

				boolean cmp_result=true;
				int cnt=0;
//				//结果比对
				

				for(int j=0;j<batchCnt;j++)
				{
					byte[] buff=new byte[length[j]];
					buffers[j]=buff;
					input.readFully(position[j], buff,0,length[j]);
				
				}
//				
				long ts2=System.currentTimeMillis();
//				
				for(int j=0;j<batchCnt;j++)
				{
					if(!Arrays.equals(buffers[j], buffer[j]))
					{
						cmp_result=false;
					}else {
						cnt++;
					}
				}
				
				System.out.println("cmpdiff:比对结果:"+cmp_result+"@校验成功次数:"+cnt+"@读取次数:"+batchCnt+"@优化后时间:"+(ts1-ts)+"@原始hdfs时间:"+(ts2-ts1));

				
			}
			
		}
		
		input.close();
		
	}
}
