package com.qst.sort.secondary;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MTMapper extends Mapper<LongWritable, Text, KVPair, IntWritable> {


	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, KVPair, IntWritable>.Context context)
			throws IOException, InterruptedException {

		// 取出一行数据
		String line = value.toString();
		String[] arr = line.split("\t");

		// 向reduce中传输数据
		KVPair kv = new KVPair();
		kv.setYear(arr[0]);
		kv.setTemp(Integer.parseInt(arr[1]));
		context.write(kv, new IntWritable(Integer.parseInt(arr[1])));
	}

}
