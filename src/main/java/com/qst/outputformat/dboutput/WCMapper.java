package com.qst.outputformat.dboutput;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, MyDBWritable, Text, IntWritable> {


	@Override
	protected void map(LongWritable key, MyDBWritable value, Mapper<LongWritable, MyDBWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.getLine();
		
		String[] arr = line.split(" ");
		
		for(String word :arr){
			context.write(new Text(word), new IntWritable(1));
			
		}
		
		System.out.println(value.getId());
		
	}
	
	

}
