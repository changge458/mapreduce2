package com.qst.join.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompKey implements WritableComparable<CompKey> {

	private int cid;
	private int flag;
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(cid);
		out.writeInt(flag);
		
	}

	public void readFields(DataInput in) throws IOException {
		cid = in.readInt();
		flag = in.readInt();
	}

	public int compareTo(CompKey o) {
		
		if(cid != o.cid){
			return cid - o.cid;
		}
		else{
			return flag - o.flag;
		}
		
	}

	@Override
	public String toString() {
		return cid + "\t" + flag;
	}

	public int getCid() {
		return cid;
	}

	public void setCid(int cid) {
		this.cid = cid;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}
	
	

}
