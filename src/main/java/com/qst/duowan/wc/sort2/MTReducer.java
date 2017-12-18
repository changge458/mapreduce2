package com.qst.duowan.wc.sort2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MTReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		// context.write(new Text("========================"), null);

		for (Text value : values) {
			context.write(value, key);
		}

	}

}
