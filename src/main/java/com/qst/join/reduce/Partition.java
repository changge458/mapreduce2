package com.qst.join.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partition extends Partitioner<CompKey, Text> {

	@Override
	public int getPartition(CompKey key, Text value, int numPartitions) {
		
		return key.getCid() % numPartitions;
		
	}
	
}
