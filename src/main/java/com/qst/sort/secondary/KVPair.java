package com.qst.sort.secondary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KVPair implements WritableComparable<KVPair>  {

	//1901	30;
	
	private String year;
	private int temp;
	
	public int compareTo(KVPair o) {
		
		String year1 = this.getYear();
		String year2 = o.getYear();
		
		int temp1 = this.getTemp();
		int temp2 = o.getTemp();
		
		if(year1.equals(year2)){
			if(temp1 == temp2){
				return 0;
			}
			return temp2 - temp1;
		}
		return year1.compareTo(year2);
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(year);
		out.writeInt(temp);
		
	}

	public void readFields(DataInput in) throws IOException {
		year = in.readUTF();
		temp = in.readInt();
	}

	@Override
	public String toString() {
		return this.year + "\t" + this.temp;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public int getTemp() {
		return temp;
	}

	public void setTemp(int temp) {
		this.temp = temp;
	}
}
