package com.qst.duowan.wc.top10;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MTMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		// 取出一行数据
		String line = value.toString();
		String[] arr = line.split("\t");

		// 向reduce中传输数据
		if(arr[0].startsWith("\t")){
			return;
		}
	
		context.write(new Text(arr[0]), new IntWritable(Integer.parseInt(arr[1])));
	}

	
	
	
}
