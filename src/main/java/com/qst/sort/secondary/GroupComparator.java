package com.qst.sort.secondary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {
	protected GroupComparator() {
		super(KVPair.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		KVPair ip1 = (KVPair) w1;
		KVPair ip2 = (KVPair) w2;
		
		String year1 = ip1.getYear();
		String year2 = ip2.getYear();
		
		return year1.compareTo(year2)  ;
	}
}
