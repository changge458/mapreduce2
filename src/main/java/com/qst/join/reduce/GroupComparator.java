package com.qst.join.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 设置分组对比器
 */
public class GroupComparator extends WritableComparator {

	public GroupComparator() {
		super(CompKey.class,null,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		CompKey k1 = (CompKey) a;
		CompKey k2 = (CompKey) b;
		
		int cid1 = k1.getCid();
		int cid2 = k2.getCid();
		
		return cid1-cid2;
		
	}
	
	
	
	

}
