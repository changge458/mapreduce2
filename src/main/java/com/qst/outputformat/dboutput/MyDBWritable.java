package com.qst.outputformat.dboutput;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class MyDBWritable implements DBWritable , Writable{
	
	private int id ;
	private String line;
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(line);
	}
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		line = in.readUTF();
		
	}
	public void write(PreparedStatement st) throws SQLException {
		st.setInt(1, id);
		st.setString(2, line);	
	}
	
	public void readFields(ResultSet rs) throws SQLException {
		id = rs.getInt(1);
		line = rs.getString(2);
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getLine() {
		return line;
	}
	public void setLine(String line) {
		this.line = line;
	}
	
	

}
