package com.qst.duowan.wc.sort2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyComparator extends WritableComparator {

	public MyComparator() {
		super(IntWritable.class, null ,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		return -super.compare(a, b);
	}
}
