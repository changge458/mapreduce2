package com.qst.duowan.wc.sort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MTReducer extends Reducer<KVPair, NullWritable, Text, IntWritable> {

	@Override
	protected void reduce(KVPair key, Iterable<NullWritable> values,
			Reducer<KVPair, NullWritable,  Text, IntWritable>.Context context) throws IOException, InterruptedException {

		//context.write(new Text("========================"), null);
		
		Text pass = new Text(key.getPass());
		IntWritable num = new IntWritable(key.getNum());
		
		context.write(pass, num);
		
	}

}
