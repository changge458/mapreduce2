package com.qst.sort.secondary;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MTReducer extends Reducer<KVPair, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(KVPair key, Iterable<IntWritable> values,
			Reducer<KVPair, IntWritable,  Text, IntWritable>.Context context) throws IOException, InterruptedException {

		context.write(new Text("========================"), null);
		
		Text year = new Text(key.getYear());
		for(IntWritable value : values){
			
			context.write(year, value);
		}
	}

}
