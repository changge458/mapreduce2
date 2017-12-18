package com.qst.outputformat.dboutput;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class MyDBWritable2 implements DBWritable , Writable{
	
	private String word;
	private int count ;
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(word);
		out.writeInt(count);
	}
	public void readFields(DataInput in) throws IOException {
		word = in.readUTF();
		count = in.readInt();
		
	}
	public void write(PreparedStatement st) throws SQLException {
		st.setString(1, word);	
		st.setInt(2, count);
	}
	
	public void readFields(ResultSet rs) throws SQLException {
		word = rs.getString(1);
		count = rs.getInt(2);
	}
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
	
	
	

}
