package com.qst.join.reduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JoinMapper extends Mapper<LongWritable, Text, CompKey, Text> {

	
	String name;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, CompKey, Text>.Context context)
			throws IOException, InterruptedException {
		FileSplit fs = (FileSplit) context.getInputSplit();
		name = fs.getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CompKey, Text>.Context context)
			throws IOException, InterruptedException {
		
		
		String line = value.toString();
		String[] arr = line.split("\t");
		
		CompKey ck = new CompKey();
		
		//当文件是客户表的情况
		if(name.contains("customers")){
			int cid = Integer.parseInt(arr[0]);
			ck.setFlag(0);
			ck.setCid(cid);
			context.write(ck, new Text(line));
		}
		//当文件是订单表的情况
		else{
			int cid = Integer.parseInt(arr[3]);
			ck.setFlag(1);
			ck.setCid(cid);
			context.write(ck, new Text(line));
		}
		
	}

}
