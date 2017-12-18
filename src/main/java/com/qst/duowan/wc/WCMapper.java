package com.qst.duowan.wc;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] arr = line.split("\t");
		if(arr.length >= 3 && arr[2] != null && arr[2].length() > 0){
			context.write(new Text(arr[2]), new IntWritable(1));
		}
	}
}
