package com.qst.duowan.wc.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MTPartition extends Partitioner<KVPair, NullWritable> {

	@Override
	public int getPartition(KVPair key, NullWritable value, int numPartitions) {

		if(key.getNum() > 1000){
			return 0;
		}
		if(key.getNum() > 100){
			return 1;
		}
		else{
			return 2;
		}
	}

}
