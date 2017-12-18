package com.qst.duowan.wc.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KVPair implements WritableComparable<KVPair> {

	// 1901 30;

	private String pass;
	private int num;

	public int compareTo(KVPair o) {

		// String pass1 = this.getPass();
		// String pass2 = o.getPass();

		
		int num1 = this.getNum();
		int num2 = o.getNum();

		
		return num2 - num1;
	}

//	@Override
//
//	public boolean equals(Object right) {
//		if (right == null)
//			return false;
//		if (this == right)
//			return true;
//		if (right instanceof KVPair) {
//			KVPair r = (KVPair) right;
//			return r.pass.equals(pass) && r.num == num;
//		} else {
//			return false;
//		}
//	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(pass);
		out.writeInt(num);

	}

	public void readFields(DataInput in) throws IOException {
		pass = in.readUTF();
		num = in.readInt();
	}

	@Override
	public String toString() {
		return this.pass + "\t" + this.num;
	}

	public String getPass() {
		return pass;
	}

	public void setPass(String year) {
		this.pass = year;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int temp) {
		this.num = temp;
	}
}
