package com.qst.duowan.wc.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MTMapper extends Mapper<LongWritable, Text, KVPair, NullWritable> {

	private int count ;
	
	
	@Override
	protected void setup(Mapper<LongWritable, Text, KVPair, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, KVPair, NullWritable>.Context context)
			throws IOException, InterruptedException {

		// 取出一行数据
		String line = value.toString();
		String[] arr = line.split("\t");

		// 向reduce中传输数据
		if(arr[0].startsWith("\t")){
			return;
		}
		KVPair kv = new KVPair();
		kv.setPass(arr[0]);
		kv.setNum(Integer.parseInt(arr[1]));
		context.write(kv, NullWritable.get());
	}
}
