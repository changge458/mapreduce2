package com.qst.duowan.wc.sort2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MTMapper extends Mapper<LongWritable, Text, IntWritable, Text> {


	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {

		// 取出一行数据
		String line = value.toString();
		String[] arr = line.split("\t");

		// 向reduce中传输数据
		if(arr[0].startsWith("\t")){
			return;
		}
		
		context.write(new IntWritable(Integer.parseInt(arr[1])), new Text(arr[0]));
	}
}
