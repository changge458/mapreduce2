package com.qst.sort.total;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.qst.duowan.wc.sort.KVPair;

public class WCMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {


		// 向reduce中传输数据
		if (key.toString().startsWith("\t")) {
			return;
		}
		
		context.write(key, value);

	}

}
