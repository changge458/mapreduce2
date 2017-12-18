package com.qst.sort.total;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WCPartition extends Partitioner<IntWritable, Text> {

	@Override
	public int getPartition(IntWritable key, Text value, int numPartitions) {
		int i = key.get();
		if(i < 250){
			return 0;
		}
		return 1;
		
	}

	
}
