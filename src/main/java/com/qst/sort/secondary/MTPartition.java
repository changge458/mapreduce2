package com.qst.sort.secondary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MTPartition extends Partitioner<KVPair, IntWritable> {

	@Override
	public int getPartition(KVPair key, IntWritable value, int numPartitions) {

		if(key.getYear().compareTo("tomas") < 0){
			return 0;
		}
		if(key.getYear().compareTo("tomson") < 0){
			return 1;
		}
		else{
			return 2;
		}
	}

}
